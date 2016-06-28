package graphql.execution.batchedrx;

import graphql.ExecutionResult;
import graphql.GraphQLError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.List;

/**
 * @author Hadrien Beaufils <hadrien.beaufils@quicksign.com>
 * @date 2016.05.28
 */
public class BatchedRxExecutionResult implements ExecutionResult {

    private static final Logger log = LoggerFactory.getLogger(BatchedRxExecutionResult.class);

    private Observable<BatchedRxExecutionResultData> mainObservable;

    public BatchedRxExecutionResult(Observable<BatchedRxExecutionResultData> mainObs) {
        mainObservable = mainObs;
    }

    public Observable<BatchedRxExecutionResultData> getObservable() {
        return mainObservable;
    }

    @Override
    public Object getData() {
        log.warn("getData() called instead of getDataObservable(), blocking (likely a bug)");
        return mainObservable.toBlocking().first().getData();
    }

    @Override
    public List<GraphQLError> getErrors() {
        log.warn("getErrors() called instead of getErrorsObservable(), blocking (likely a bug)");
        return mainObservable.toBlocking().first().getErrors();
    }
}
