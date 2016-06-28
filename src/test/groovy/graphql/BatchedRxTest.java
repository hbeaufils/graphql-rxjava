package graphql;

import graphql.SchemaFixture.Company;
import graphql.SchemaFixture.HistoryLine;
import graphql.SchemaFixture.Product;
import graphql.SchemaFixture.Transaction;
import graphql.SchemaFixture.TypeName;
import graphql.execution.ExecutionStrategy;
import graphql.execution.batched.BatchedDataFetcher;
import graphql.execution.batchedrx.*;
import graphql.schema.*;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.SchemaFixture.*;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test batched Rx execution strategy.
 * 
 * TODO test multi-threaded data fetcher calls (create a data fetcher wrapper which delegates call in another thread)
 */
public class BatchedRxTest {

    private static final Logger log = LoggerFactory.getLogger( BatchedRxTest.class );
    
    private static ExecutionStrategy executionStrategy = new BatchedRxExecutionStrategy();

    // 'companies' fetcher: attribute of root, data to fetch is a list of company
    public final BatchedDataFetcher baseCompaniesDf = new BatchedDataFetcher() {
        
        @Override
        public Observable<List<Object>> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on companies data fetcher");
            
            List<Object> dataBatch = new ArrayList<>();
            Map<String,Object> args = env.getArguments();
                        
            if (args.get("id") == null) {
                dataBatch.add( Arrays.asList( c1, c2 ) );
            }
            else if ("0".equals( args.get("id") )) {
                dataBatch.add( Arrays.asList( c1 ) );
            }
            else if ("1".equals( args.get("id") )) {
                dataBatch.add( Arrays.asList( c2 ) );
            }
            else {
                dataBatch.add( new ArrayList<>());
            }
            return Observable.just( dataBatch );
        }
    };

    // 'companies' fetcher: attribute of root, data to fetch is a list of company
    public final DataFetcher unbatchedCompaniesDf = new DataFetcher() {

        @Override
        public Observable<Object> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on companies unbatched data fetcher");

            Map<String,Object> args = env.getArguments();

            if (args.get("id") == null) {
                return Observable.just( Arrays.asList( c1, c2 ) );
            }
            else if ("0".equals( args.get("id") )) {
                return Observable.just( Arrays.asList( c1 ) );
            }
            else if ("1".equals( args.get("id") )) {
                return Observable.just( Arrays.asList( c2 ) );
            }
            return Observable.just( null );
        }
    };

    // 'product' fetcher: attribute of company, data to fetch is a single product
    public final BatchedDataFetcher baseProductDf = new BatchedDataFetcher() {

        @Override
        public Observable<List<Object>> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on product data fetcher");
            
            Object srcObj = env.getSource();
            if (srcObj instanceof List) { // Batched data fetcher

                List sources = (List) srcObj;
                List<Object> dataBatch = new ArrayList<>();
                sources.forEach( src -> {

                    if (src instanceof Company) {
                        Company c = (Company) src;
                        
                        if ("0".equals( c.getId() )) {
                            dataBatch.add( p1 );
                        }
                        else if ("1".equals( c.getId() )) {
                            dataBatch.add( p2 );
                        }
                        else {
                            dataBatch.add( null );
                        }
                    }
                    else {
                        dataBatch.add( null );
                    }
                });
                return Observable.just( dataBatch );
            }
            return null;
        }
    };

    // 'product' fetcher: attribute of company, data to fetch is a single product
    public final DataFetcher unbatchedProductDf = new DataFetcher() {

        @Override
        public Observable<Object> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on product data fetcher");

            Object srcObj = env.getSource();
            if (srcObj instanceof Company) {
                Company c = (Company) srcObj;

                if ("0".equals( c.getId() )) {
                    return Observable.just( p1 );
                }
                else if ("1".equals( c.getId() )) {
                    return Observable.just( p2 );
                }
                return Observable.just( null );
            }
            return Observable.just( null );
        }
    };

    // 'transactions' fetcher: attribute of product, data to fetch is a list of transaction
    public final BatchedDataFetcher baseTransactionsDf = new BatchedDataFetcher() {

        @Override
        public Observable<List<Object>> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on transactions data fetcher");
            
            Object srcObj = env.getSource();
            if (srcObj instanceof List) { // Batched data fetcher
                
                List sources = (List) srcObj;
                List<Object> dataBatch = new ArrayList<>();
                sources.forEach( src -> {

                    if (src instanceof Product) {
                        Product p = (Product) src;

                        if ("123".equals( p.getSerialNumber() )) {
                            dataBatch.add( Arrays.asList( t1a, t1b ) );
                        }
                        else if ("456".equals( p.getSerialNumber() )) {
                            dataBatch.add( Arrays.asList( t2a, t2b ) );
                        }
                        else {
                            dataBatch.add( new ArrayList<>());
                        }
                    }
                    else {
                        dataBatch.add( null );
                    }
                });
                return Observable.just( dataBatch );
            }
            return null;
        }
    };

    // 'historyLines' fetcher: attribute of transaction, data to fetch is a list of historyLine
    public final BatchedDataFetcher baseHistoryLinesDf = new BatchedDataFetcher() {

        @Override
        public Observable<List<Object>> get( DataFetchingEnvironment env ) {

            log.debug("Calling get on history lines data fetcher");
            
            Map<String,Object> args = env.getArguments();
            Object srcObj = env.getSource();
            if (srcObj instanceof List) { // Batched data fetcher

                List sources = (List) srcObj;
                List<Object> dataBatch = new ArrayList<>();
                sources.forEach( src -> {

                    if (src instanceof Transaction) {
                        Transaction tx = (Transaction) src;

                        List<HistoryLine> subList = new ArrayList<>();
                        
                        if (args != null && HistoryLine.Type.CREATE.toString().equals( args.get("type") )) {
                            subList.add( creationEvent );
                        }
                        
                        if (tx.getToken() != null && tx.getToken().contains("AAAA")) {
                            if (args != null && HistoryLine.Type.UPDATE.toString().equals( args.get("type") )) {
                                subList.add( updateForTransactionsAAAA );
                            }
                        }
                        dataBatch.add( subList );
                    }
                    else {
                        dataBatch.add( null );
                    }
                });
                return Observable.just( dataBatch );
            }
            return null;
        }
    };
    
    /**
     * Generate a new schema instance with given data fetchers.
     *
     * @param companyDf
     * @param productDf
     * @param transactionDf
     * @param historyLineDf
     * @return
     */
    private GraphQLSchema getGraphQLSchema(
            DataFetcher companyDf, DataFetcher productDf, DataFetcher transactionDf, DataFetcher historyLineDf
    ) {
        GraphQLObjectType historyLineType = newObject()
                .name( TypeName.HISTORY_LINE )
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("date")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("type")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("description")
                        .build())
                .build();
        
        GraphQLObjectType transactionType = newObject()
                .name( TypeName.TRANSACTION )
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("token")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("email")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("fullName")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("fullName")
                        .build())
                .field(newFieldDefinition()
                        .type( new GraphQLList( historyLineType ))
                        .name("historyLines")
                        .argument( new GraphQLArgument("type", GraphQLString ))
                        .dataFetcher(historyLineDf)
                        .build())
                .build();

        GraphQLObjectType productType = newObject()
                .name( TypeName.PRODUCT )
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("name")
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("serialNumber")
                        .build())
                .field(newFieldDefinition()
                        .type( new GraphQLList( transactionType ))
                        .name("transactions")
                        .dataFetcher(transactionDf)
                        .build())
                .build();

        GraphQLObjectType companyType = newObject()
                .name( TypeName.COMPANY )
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("id")
                        .dataFetcher(environment -> {
                            if (environment.getSource() instanceof Company) {
                                Company company = (Company) environment.getSource();
                                return company.getId();
                            }
                            return null;
                        })
                        .build())
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("name")
                        .dataFetcher(environment -> {
                            if (environment.getSource() instanceof Company) {
                                Company company = (Company) environment.getSource();
                                return company.getName();
                            }
                            return null;
                        })
                        .build())
                .field(newFieldDefinition()
                        .type(productType)
                        .name("product")
                        .dataFetcher(productDf)
                        .build())
                .build();

        GraphQLObjectType queryType = newObject()
                .name("Query")
                .field(newFieldDefinition()
                        .type( new GraphQLList( companyType ) )
                        .name("companies")
                        .argument( new GraphQLArgument("id", GraphQLString ))
                        .dataFetcher(companyDf)
                        .build())
                .field(newFieldDefinition()
                        .type( new GraphQLList( productType ) )
                        .name("products")
                        .argument( new GraphQLArgument("id", GraphQLString ))
                        .dataFetcher(productDf)
                        .build())
                .build();

        return GraphQLSchema.newSchema()
                .query(queryType)
                .build();
    }

    @Test
    public void test_1_should_complete() {

        DataFetcher companyDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher transactionDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher productDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher historyLineDf = Mockito.mock(BatchedDataFetcher.class);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Mock data fetchers

        Mockito.when(companyDf.get(Mockito.any())).thenReturn(null);
        Mockito.when(productDf.get(Mockito.any())).thenReturn(null);
        Mockito.when(transactionDf.get(Mockito.any())).thenReturn(null);
        Mockito.when(historyLineDf.get(Mockito.any())).thenReturn(null);

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy ).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);
        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
    }

    @Test
    public void test_2_should_complete_with_valid_result_data() {

        BatchedDataFetcher companyDf = Mockito.spy( baseCompaniesDf );
        BatchedDataFetcher productDf = Mockito.spy( baseProductDf );
        BatchedDataFetcher transactionDf = Mockito.spy( baseTransactionsDf );
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy ).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue("There should not be any GraphQL error: "+ resultData.getErrors(), resultData.getErrors() == null || resultData.getErrors().isEmpty());

        Mockito.verify(companyDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());
    }

    @Test
    public void test_3_should_fetch_data_correctly_with_complex_requests() {

        BatchedDataFetcher companyDf = Mockito.spy( baseCompaniesDf );
        BatchedDataFetcher productDf = Mockito.spy( baseProductDf );
        BatchedDataFetcher transactionDf = Mockito.spy( baseTransactionsDf );
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );
        
        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy).execute("{"+
                "  c1:companies(id:\"0\") {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "        email"+
                "        fullName"+
                "        historyLines(type:\"UPDATE\") {"+
                "          date" +
                "          type" +
                "          description"+
                "        }"+
                "      }"+
                "    }"+
                "  }"+
                "  c2:companies(id:\"1\") {"+
                "    product {"+
                "      name"+
                "      serialNumber"+
                "    }"+
                "  }"+
                "  companies {"+
                "    name"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue("There should not be any GraphQL error: "+ resultData.getErrors(), resultData.getErrors() == null || resultData.getErrors().isEmpty());

        Mockito.verify(companyDf, Mockito.times(3)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(2)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(1)).get(Mockito.any());
    }

    @Test
    public void test_4_should_result_in_bad_list_size_for_transactionsDf() {

        DataFetcher transactionDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher productDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher companyDf = Mockito.mock(BatchedDataFetcher.class);
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Mock data fetchers

        Mockito.when(companyDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( Arrays.asList(c1,c2) )));
        Mockito.when(productDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( p1, p2 ) ));

        // Error case
        Mockito.when(transactionDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( Arrays.asList(t1a, t1b)) ));

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue(
                "There should be 1 and only 1 GraphQL error: "+ resultData.getErrors(),
                resultData.getErrors() != null && resultData.getErrors().size() == 1
        );
        assertTrue(
                "GraphQL error should be an InvalidFetchedDataException: "+ resultData.getErrors().get(0),
                resultData.getErrors().get(0) instanceof InvalidFetchedDataException
        );
        assertEquals(
                "Error message should match the 'source/result size mismatch' error",
                ((InvalidFetchedDataException) resultData.getErrors().get(0)).getException().getMessage(),
                "Source/result size mismatch in 'transactions' data fetcher from type '"+  TypeName.PRODUCT +"' (expecting 2, got 1)"
        );

        Mockito.verify(companyDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());
    }

    @Test
    public void test_5_should_result_in_an_invalid_fetched_data_for_transactionsDf() {

        DataFetcher transactionDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher productDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher companyDf = Mockito.mock(BatchedDataFetcher.class);
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Mock data fetchers

        Mockito.when(companyDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( Arrays.asList(c1,c2) )));
        Mockito.when(productDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( p1, p2 ) ));

        // Error case
        Mockito.when(transactionDf.get(Mockito.any())).thenReturn( new Object());

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue(
                "There should be 1 and only 1 GraphQL error: "+ resultData.getErrors(),
                resultData.getErrors() != null && resultData.getErrors().size() == 1
        );
        assertTrue(
                "GraphQL error should be an InvalidFetchedDataException: "+ resultData.getErrors().get(0),
                resultData.getErrors().get(0) instanceof InvalidFetchedDataException
        );
        assertEquals(
                "Error message should match the 'source/result size mismatch' error",
                ((InvalidFetchedDataException) resultData.getErrors().get(0)).getException().getMessage(),
                "Invalid data fetched by 'transactions' data fetcher from type '"+  TypeName.PRODUCT +"' (expecting an Observable of "
                        + "List, but got a class java.lang.Object object)"
        );

        Mockito.verify(companyDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());
    }

    @Test
    public void test_6_should_result_in_data_fetching_exception_for_productDf() {

        DataFetcher transactionDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher productDf = Mockito.mock(BatchedDataFetcher.class);
        DataFetcher companyDf = Mockito.mock(BatchedDataFetcher.class);
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Mock data fetchers

        Mockito.when(companyDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( Arrays.asList(c1,c2) )));
        Mockito.when(productDf.get(Mockito.any())).thenThrow(new RuntimeException("Data fetcher mocked error"));

        // Error case
        Mockito.when(transactionDf.get(Mockito.any())).thenReturn( Observable.just( Arrays.asList( Arrays.asList(t1a, t1b)) ));

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue(
                "There should be 1 and only 1 GraphQL error: "+ resultData.getErrors(),
                resultData.getErrors() != null && resultData.getErrors().size() == 1
        );
        assertTrue(
                "GraphQL error should be an ExceptionWhileDataFetching: "+ resultData.getErrors().get(0),
                resultData.getErrors().get(0) instanceof ExceptionWhileDataFetching
        );
        assertEquals(
                "Error message should match the 'mocked data fetcher' exception message",
                ((ExceptionWhileDataFetching) resultData.getErrors().get(0)).getException().getMessage(),
                "Data fetcher mocked error"
        );

        Mockito.verify(companyDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());
    }

	/**
     * Copy of #test_2_should_complete_with_valid_result_data() with unbatched data
     * fetchers for companies and products.
     */
    @Test
    public void test_7_should_support_unbatched_data_fetcher() {

        DataFetcher companyDf = Mockito.spy( unbatchedCompaniesDf );
        DataFetcher productDf = Mockito.spy( unbatchedProductDf );
        BatchedDataFetcher transactionDf = Mockito.spy( baseTransactionsDf );
        BatchedDataFetcher historyLineDf = Mockito.spy(baseHistoryLinesDf);

        GraphQLSchema schema = getGraphQLSchema( companyDf, productDf, transactionDf, historyLineDf );

        // Check calls

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        // Plan execution

        ExecutionResult executionResult = new GraphQL(schema, executionStrategy ).execute("{"+
                "  companies {"+
                "    name"+
                "    product {"+
                "      serialNumber"+
                "      transactions {"+
                "        token"+
                "      }"+
                "    }"+
                "  }"+
                "}"
        );

        Mockito.verify(companyDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(0)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());

        assertTrue("Execution result's not an Rx Observable result", executionResult instanceof BatchedRxExecutionResult);

        BatchedRxExecutionResult execRes = (BatchedRxExecutionResult) executionResult;

        // Subscribe and test

        TestSubscriber<BatchedRxExecutionResultData> testSubscriber = new TestSubscriber<>();

        execRes.getObservable().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertTerminalEvent();
        List<BatchedRxExecutionResultData> resultDataLst = testSubscriber.getOnNextEvents();

        assertTrue("There should be 1 and only 1 result data", resultDataLst != null && resultDataLst.size() == 1);

        BatchedRxExecutionResultData resultData = resultDataLst.get(0);

        assertTrue("Result data should be a Map: "+ resultData.getData().getClass(), resultData.getData() instanceof Map);

        log.info("Data: " + resultData.getData());
        log.info("Errors: " + resultData.getErrors());

        assertTrue("There should not be any GraphQL error: "+ resultData.getErrors(), resultData.getErrors() == null || resultData.getErrors().isEmpty());

        Mockito.verify(companyDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(productDf, Mockito.times(2)).get(Mockito.any());
        Mockito.verify(transactionDf, Mockito.times(1)).get(Mockito.any());
        Mockito.verify(historyLineDf, Mockito.times(0)).get(Mockito.any());
    }
}
