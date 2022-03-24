package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.AmazonServiceException;

public class GenericApiGatewayException extends AmazonServiceException {
    public GenericApiGatewayException(String errorMessage) {
        super(errorMessage);
    }

    public GenericApiGatewayException(String errorMessage, Exception cause) {
        super(errorMessage, cause);
    }
}