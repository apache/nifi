package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.http.HttpMethodName;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class GenericApiGatewayRequest {

    private final HttpMethodName httpMethod;
    private final String resourcePath;
    private final InputStream body;
    private final Map<String, String> headers;
    private final Map<String, List<String>> parameters;

    public GenericApiGatewayRequest(HttpMethodName httpMethod, String resourcePath,
                                    InputStream body, Map<String, String> headers,
                                    Map<String, List<String>> parameters) {
        this.httpMethod = httpMethod;
        this.resourcePath = resourcePath;
        this.body = body;
        this.headers = headers;
        this.parameters = parameters;
    }

    public HttpMethodName getHttpMethod() {
        return httpMethod;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public InputStream getBody() {
        return body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, List<String>> getParameters() {
        return parameters;
    }
}
