package graphql.execution.batchedrx;

import graphql.GraphQLError;

import java.util.List;

/**
 * Wrapper for execution result.
 * 
 * @author Hadrien Beaufils <hadrien.beaufils@quicksign.com>
 * @date 2016.05.28
 */
public class BatchedRxExecutionResultData {

    private Object data;
    private List<GraphQLError> errors;

    public BatchedRxExecutionResultData(Object data, List<GraphQLError> errors) {
        this.data = data;
        this.errors = errors;
    }

    public Object getData() {
        return data;
    }

    public List<GraphQLError> getErrors() {
        return errors;
    }
}
