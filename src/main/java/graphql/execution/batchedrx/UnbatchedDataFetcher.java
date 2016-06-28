package graphql.execution.batchedrx;


import graphql.execution.batched.BatchedDataFetcher;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

/**
 * Given a normal data fetcher as a delegate, uses that fetcher in a batched
 * context by iterating through each source value and calling the delegate.
 * 
 * This implementation's based on the original {@link graphql.execution.batched.UnbatchedDataFetcher}
 * to add support for unbatched data fetcher returning Rx Observable.
 * 
 * @author Hadrien Beaufils <hadrien.beaufils@quicksign.com>
 * @date 2016.06.17
 */
public class UnbatchedDataFetcher implements BatchedDataFetcher {

    private final DataFetcher delegate;

    public UnbatchedDataFetcher(DataFetcher delegate) {
        assert !(delegate instanceof BatchedDataFetcher);
        this.delegate = delegate;
    }


    @Override
    public Object get(DataFetchingEnvironment environment) {
        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) environment.getSource();
        List<Observable<Object>> results = new ArrayList<>();
        for (Object source : sources) {
            DataFetchingEnvironment singleEnv = new DataFetchingEnvironment(
                    source,
                    environment.getArguments(),
                    environment.getContext(),
                    environment.getFields(),
                    environment.getFieldType(),
                    environment.getParentType(),
                    environment.getGraphQLSchema());
            
            Object res = delegate.get(singleEnv);
            if (res instanceof Observable) {
                results.add((Observable) res);
            } else {
                results.add( Observable.just(res) );
            }
        }
        return Observable.from(results)
                .flatMap(o -> o)
                .toList();
    }
}
