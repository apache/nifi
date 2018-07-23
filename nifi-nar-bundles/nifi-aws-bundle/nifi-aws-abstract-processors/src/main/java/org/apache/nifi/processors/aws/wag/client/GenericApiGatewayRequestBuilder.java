package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.http.HttpMethodName;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class GenericApiGatewayRequestBuilder {
    private HttpMethodName httpMethod;
    private String resourcePath;
    private InputStream body;
    private Map<String, String> headers;
    private Map<String, List<String>> parameters;

    public GenericApiGatewayRequestBuilder withHttpMethod(HttpMethodName name) {
        httpMethod = name;
        return this;
    }

    public GenericApiGatewayRequestBuilder withResourcePath(String path) {
        resourcePath = path;
        return this;
    }

    public GenericApiGatewayRequestBuilder withBody(InputStream content) {
        this.body = content;
        return this;
    }

    public GenericApiGatewayRequestBuilder withHeaders(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public GenericApiGatewayRequestBuilder withParameters(Map<String,List<String>> parameters) {
        this.parameters = parameters;
        return this;
    }

    public boolean hasBody() {
        return this.body != null;
    }

    public GenericApiGatewayRequest build() {
        Validate.notNull(httpMethod, "HTTP method");
        Validate.notEmpty(resourcePath, "Resource path");
        return new GenericApiGatewayRequest(httpMethod, resourcePath, body, headers, parameters);
    }
}