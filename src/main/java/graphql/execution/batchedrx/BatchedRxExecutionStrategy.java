package graphql.execution.batchedrx;

import graphql.*;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStrategy;
import graphql.execution.batched.BatchedDataFetcher;
import graphql.language.Field;
import graphql.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Strategy to fetch data in batched mode for RxJava data fetcher. It's based
 * on the original {@link graphql.execution.batched.BatchedExecutionStrategy}.
 *
 * It uses RxJava to add a support for Observable data from BatchedDataFetcher, and return an Observable result.
 * The whole process makes data fetching non-blocking.
 *
 * Batched and unbatched data fetcher are supported, and both can return direct data or Observable.
 * 
 * @author <a href="mailto:hadrien.beaufils@quicksign.com">Hadrien Beaufils</a>
 * created on 2016.05.28
 */
public class BatchedRxExecutionStrategy extends ExecutionStrategy {

    private static final Logger log = LoggerFactory.getLogger(BatchedRxExecutionStrategy.class);

    private final BatchedDataFetcherFactory batchingFactory = new BatchedDataFetcherFactory();

    @Override
    public ExecutionResult execute(
            ExecutionContext executionContext, GraphQLObjectType parentType,
            Object source, Map<String, List<Field>> fields
    ) {

        GraphQLExecutionNodeDatum data = new GraphQLExecutionNodeDatum(new LinkedHashMap<>(), source);
        GraphQLExecutionNode root = new GraphQLExecutionNode(parentType, fields, singletonList(data));
        return execute(executionContext, root);
    }

	/**
     * Start recursive execution of nodes with the root, then reduce results to
     * regroup them in a single resultData object.
     *
     * @param executionContext
     * @param root
     * @return An Observable ExecutionResult.
     */
    private ExecutionResult execute(ExecutionContext executionContext, GraphQLExecutionNode root) {

        Observable<BatchedRxExecutionResultData> obs = Observable.just(root)
                .flatMap( node -> executeNode( executionContext, node ))
                .reduce( new HashMap<>(), (stringObjectHashMap, graphQLExecutionNodes) -> {
                    return null;
                })
                .map( uselessMap -> {
                    return new BatchedRxExecutionResultData(
                            getOnlyElement(root.getData()).getParentResult(),
                            executionContext.getErrors()
                    );
                });
        
        return new BatchedRxExecutionResult( obs );
    }

	/**
     * Execute a node and recursively merge with children nodes execution.
     *
     * @param executionContext
     * @param node
     * @return
     */
    private Observable<GraphQLExecutionNode> executeNode(
            ExecutionContext executionContext, GraphQLExecutionNode node
        ) {

        List<Observable<List<GraphQLExecutionNode>>> observables = new ArrayList<>();

        for (String fieldName : node.getFields().keySet()) {
            List<Field> fieldList = node.getFields().get(fieldName);

            Observable<List<GraphQLExecutionNode>> childNodes = resolveField(
                    executionContext, node.getParentType(), node.getData(), fieldName, fieldList
            );
            observables.add(childNodes);

            log.trace("Field resolved and added in observables: {}", fieldName );
        }

        return Observable.merge(
                Observable.just(node),
                Observable
                        .from(observables)
                        .flatMap( listObservable -> listObservable )
                        .flatMap( subNodes -> Observable.from( subNodes ))
                        .flatMap( subNode -> executeNode( executionContext, subNode ))
        );
    }

    private GraphQLExecutionNodeDatum getOnlyElement(List<GraphQLExecutionNodeDatum> list) {
        return list.get(0);
    }

    // Use the data.source objects to fetch
    // Use the data.parentResult objects to put values into.  These are either primitives or empty maps
    // If they were empty maps, we need that list of nodes to process

	/**
     * Retrieve field definition, fetch corresponding data, and then fill values into result objects.
     *
     * @param executionContext
     * @param parentType
     * @param nodeData
     * @param fieldName
     * @param fields
     * @return
     */
    private Observable<List<GraphQLExecutionNode>> resolveField(
            ExecutionContext executionContext, GraphQLObjectType parentType,
            List<GraphQLExecutionNodeDatum> nodeData, String fieldName, List<Field> fields
        ) {

        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext.getGraphQLSchema(), parentType, fields.get(0));
        if (fieldDef == null) {
            return Observable.just( Collections.emptyList() );
        }
        Observable<List<GraphQLExecutionNodeValue>> valuesObs = fetchData(
                executionContext, parentType, nodeData, fields, fieldDef
        );

        return valuesObs.map( values -> completeValues(
                executionContext, parentType, values, fieldName, fields, fieldDef.getType()
        ));
    }

	/**
     * Updates parents and returns new nodes.
     *
     * @param executionContext
     * @param parentType
     * @param values
     * @param fieldName
     * @param fields
     * @param outputType
     * @return New execution node for children.
	 */
    private List<GraphQLExecutionNode> completeValues(
            ExecutionContext executionContext, GraphQLObjectType parentType,
            List<GraphQLExecutionNodeValue> values, String fieldName, List<Field> fields,
            GraphQLOutputType outputType
        ) {

        GraphQLType fieldType = handleNonNullType(outputType,values,parentType,fields);

        if (isPrimitive(fieldType)) {
            handlePrimitives(values, fieldName, fieldType);
            return Collections.emptyList();
        } else if (isObject(fieldType)) {
            return handleObject(executionContext, values, fieldName, fields, fieldType);
        } else if (isList(fieldType)) {
            return handleList(executionContext, values, fieldName, fields, parentType, (GraphQLList) fieldType);
        } else {
            throw new IllegalArgumentException("Unrecognized type: " + fieldType);
        }
    }

    @SuppressWarnings("unchecked")
    private List<GraphQLExecutionNode> handleList(
            ExecutionContext executionContext, List<GraphQLExecutionNodeValue> values,
            String fieldName, List<Field> fields, GraphQLObjectType parentType, GraphQLList listType
        ) {

        List<GraphQLExecutionNodeValue> flattenedNodeValues = new ArrayList<>();

        for (GraphQLExecutionNodeValue value : values) {
            if (value.getValue() == null) {
                value.getResultContainer().putResult(fieldName, null);
            } else {
                GraphQLExecutionResultList flattenedDatum = value.getResultContainer().createAndPutEmptyChildList(
                        fieldName);
                for (Object rawValue : (List<Object>) value.getValue()) {
                    flattenedNodeValues.add(new GraphQLExecutionNodeValue(flattenedDatum, rawValue));
                }
            }
        }

        GraphQLOutputType subType = (GraphQLOutputType) listType.getWrappedType();
        return completeValues(executionContext, parentType, flattenedNodeValues, fieldName, fields, subType);

    }

    private List<GraphQLExecutionNode> handleObject(
            ExecutionContext executionContext, List<GraphQLExecutionNodeValue> values,
            String fieldName, List<Field> fields, GraphQLType fieldType
        ) {

        ChildDataCollector collector = createAndPopulateChildData(values, fieldName, fieldType);

        List<GraphQLExecutionNode> childNodes =
                createChildNodes(executionContext, fields, collector);

        return childNodes;
    }

    private List<GraphQLExecutionNode> createChildNodes(
            ExecutionContext executionContext, List<Field> fields, ChildDataCollector collector
        ) {

        List<GraphQLExecutionNode> childNodes = new ArrayList<>();

        for (ChildDataCollector.Entry entry : collector.getEntries()) {
            Map<String, List<Field>> childFields = getChildFields(executionContext, entry.getObjectType(), fields);
            childNodes.add(new GraphQLExecutionNode(entry.getObjectType(), childFields, entry.getData()));
        }
        return childNodes;
    }

    private ChildDataCollector createAndPopulateChildData(
            List<GraphQLExecutionNodeValue> values, String fieldName, GraphQLType fieldType
        ) {

        ChildDataCollector collector = new ChildDataCollector();
        for (GraphQLExecutionNodeValue value : values) {
            if (value.getValue() == null) {
                // We hit a null, insert the null and do not create a child
                value.getResultContainer().putResult(fieldName, null);
            } else {
                GraphQLExecutionNodeDatum childDatum = value.getResultContainer().createAndPutChildDatum(fieldName, value.getValue());
                GraphQLObjectType graphQLObjectType = getGraphQLObjectType(fieldType, value.getValue());
                collector.putChildData(graphQLObjectType, childDatum);
            }
        }
        return collector;
    }

    private GraphQLType handleNonNullType(
            GraphQLType fieldType, List<GraphQLExecutionNodeValue> values,
            /*Nullable*/ GraphQLObjectType parentType, /*Nullable*/ List<Field> fields
        ) {

        if (isNonNull(fieldType)) {
            for (GraphQLExecutionNodeValue value : values) {
                if (value.getValue() == null) {
                    throw new GraphQLException("Found null value for non-null type with parent: '"
                            + parentType.getName() + "' for fields: " + fields);
                }
            }
            while (isNonNull(fieldType)) {
                fieldType = ((GraphQLNonNull) fieldType).getWrappedType();
            }
        }
        return fieldType;
    }

    private boolean isNonNull(GraphQLType fieldType) {
        return fieldType instanceof GraphQLNonNull;
    }

    private Map<String, List<Field>> getChildFields(
            ExecutionContext executionContext, GraphQLObjectType resolvedType, List<Field> fields
        ) {

        Map<String, List<Field>> subFields = new LinkedHashMap<>();
        List<String> visitedFragments = new ArrayList<>();
        for (Field field : fields) {
            if (field.getSelectionSet() == null) {
                continue;
            }
            fieldCollector.collectFields(
                    executionContext, resolvedType, field.getSelectionSet(), visitedFragments, subFields
            );
        }
        return subFields;
    }

    private GraphQLObjectType getGraphQLObjectType(GraphQLType fieldType, Object value) {

        GraphQLObjectType resolvedType = null;
        if (fieldType instanceof GraphQLInterfaceType) {
            resolvedType = resolveType((GraphQLInterfaceType) fieldType, value);
        } else if (fieldType instanceof GraphQLUnionType) {
            resolvedType = resolveType((GraphQLUnionType) fieldType, value);
        } else if (fieldType instanceof GraphQLObjectType) {
            resolvedType = (GraphQLObjectType) fieldType;
        }
        return resolvedType;
    }

    private void handlePrimitives(
            List<GraphQLExecutionNodeValue> values, String fieldName, GraphQLType type
        ) {

        for (GraphQLExecutionNodeValue value : values) {
            Object coercedValue = coerce(type, value.getValue());
            //6.6.1 http://facebook.github.io/graphql/#sec-Field-entries
            if (coercedValue instanceof Double && ((Double) coercedValue).isNaN()) {
                coercedValue = null;
            }
            value.getResultContainer().putResult(fieldName, coercedValue);
        }
    }

    private Object coerce(GraphQLType type, Object value) {

        if (type instanceof GraphQLEnumType) {
            return ((GraphQLEnumType) type).getCoercing().serialize(value);
        } else {
            return ((GraphQLScalarType) type).getCoercing().serialize(value);
        }
    }

    private boolean isList(GraphQLType type) {
        return type instanceof GraphQLList;
    }

    private boolean isPrimitive(GraphQLType type) {
        return type instanceof GraphQLScalarType || type instanceof GraphQLEnumType;
    }

    private boolean isObject(GraphQLType type) {
        return type instanceof GraphQLObjectType ||
                type instanceof GraphQLInterfaceType ||
                type instanceof GraphQLUnionType;
    }

	/**
     * Fetch data for given field definition using node data as batched sources.
     * Excepted result is a list of values that matches this source list.
     *
     * @param executionContext
     * @param parentType
     * @param nodeData
     * @param fields
     * @param fieldDef
     * @return
     */
    @SuppressWarnings("unchecked")
    private Observable<List<GraphQLExecutionNodeValue>> fetchData(
            ExecutionContext executionContext, GraphQLObjectType parentType,
            List<GraphQLExecutionNodeDatum> nodeData, List<Field> fields, GraphQLFieldDefinition fieldDef
        ) {

        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(
                fieldDef.getArguments(), fields.get(0).getArguments(), executionContext.getVariables());

        List<Object> sources = nodeData
                .stream()
                .map( GraphQLExecutionNodeDatum::getSource )
                .collect( Collectors.toList() );

        DataFetchingEnvironment environment = new DataFetchingEnvironment(
                sources,
                argumentValues,
                executionContext.getRoot(),
                fields,
                fieldDef.getType(),
                parentType,
                executionContext.getGraphQLSchema()
        );

        Observable<List<Object>> valuesObs = null;
        try {
            Object tmpObj = getDataFetcher(fieldDef).get(environment);
            if (tmpObj != null) {
                if (tmpObj instanceof Observable) {
                    valuesObs = (Observable<List<Object>>) tmpObj;
                }
                else if (tmpObj instanceof List) {
                    valuesObs = Observable.just( (List<Object>) tmpObj );
                }
                else {
                    Exception e = new RuntimeException(""+
                            "Invalid data fetched by '"+ fieldDef.getName()+
                            "' data fetcher from type '"+ parentType.getName() +"' "+
                            "(expecting an Observable of List, but got a "+ tmpObj.getClass() +" object)"
                    );
                    log.error("Error with fetched data", e);
                    executionContext.addError( new InvalidFetchedDataException(e));
                }
            }
        }
        catch (Exception e) {
            log.error("Exception while fetching data", e);
            executionContext.addError(new ExceptionWhileDataFetching(e));
        }
        
        if (valuesObs == null) {
            valuesObs = Observable.just( buildNullValueList( nodeData.size() ));
        }

        return valuesObs.map( values -> {

            if (nodeData.size() != values.size()) {
                
                Exception e = new RuntimeException(
                        "Source/result size mismatch in '"+ fieldDef.getName() +
                                "' data fetcher from type '"+ parentType.getName() +"' "+
                                "(expecting "+ nodeData.size() +", got "+ values.size() +")"
                );
                log.error("Error with fetched data size", e);
                executionContext.addError( new InvalidFetchedDataException(e));
                
                values = buildNullValueList( nodeData.size() );
            }

            List<GraphQLExecutionNodeValue> retVal = new ArrayList<>();
            for (int i = 0; i < nodeData.size(); i++) {
                retVal.add(new GraphQLExecutionNodeValue(nodeData.get(i), values.get(i)));
            }
            
            return retVal;
        });
    }

    /**
     * @param nbOfBatchedData Size of the list to return.
     * @return A list of null values.
     */
    private List<Object> buildNullValueList( int nbOfBatchedData ) {

        List<Object> values = new ArrayList<>();
        for (int iBatch = 0; iBatch < nbOfBatchedData; ++iBatch) {
            values.add(null);
        }
        return values;
    }

    private BatchedDataFetcher getDataFetcher(GraphQLFieldDefinition fieldDef) {
        DataFetcher supplied = fieldDef.getDataFetcher();
        return batchingFactory.create(supplied);
    }
}
