package org.apache.nifi.remote.protocol;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.RequestExpiredException;

public class ProcessingResult {
    private final int fileCount;
    private final Exception problem;

    public ProcessingResult(final int fileCount) {
        this.fileCount = fileCount;
        this.problem = null;
    }

    public ProcessingResult(final Exception problem) {
        this.fileCount = 0;
        this.problem = problem;
    }

    /**
     * Returns processed FlowFile count
     * @return the number of FlowFiles processed
     * @throws NotAuthorizedException the request was not authorized
     * @throws BadRequestException the request was not acceptable
     * @throws RequestExpiredException the request got expired
     */
    public int getFileCount() throws NotAuthorizedException, BadRequestException, RequestExpiredException {

        try {
            if (problem == null) {
                return fileCount;
            } else {
                throw problem;
            }
        } catch (final NotAuthorizedException | BadRequestException | RequestExpiredException e) {
            throw e;
        } catch (final ProtocolException e) {
            throw new BadRequestException(e);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }
}
