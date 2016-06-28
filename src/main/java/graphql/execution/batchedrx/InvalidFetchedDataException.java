package graphql.execution.batchedrx;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.language.SourceLocation;

import java.util.List;

/**
 * @author Hadrien Beaufils <hadrien.beaufils@quicksign.com>
 * @date 2016.06.08
 */
public class InvalidFetchedDataException implements GraphQLError {

	private final Exception exception;

	public InvalidFetchedDataException(Exception exception) {
		this.exception = exception;
	}

	public Exception getException() {
		return exception;
	}


	@Override
	public String getMessage() {
		return "Invalid data fetched: "+ exception.toString();
	}

	@Override
	public List<SourceLocation> getLocations() {
		return null;
	}

	@Override
	public ErrorType getErrorType() {
		return ErrorType.DataFetchingException;
	}

	@Override
	public String toString() {
		return "InvalidFetchedDataException{exception=" + exception +'}';
	}
}
