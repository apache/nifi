package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.http.HttpResponse;
import com.amazonaws.util.IOUtils;
import java.io.IOException;

public class GenericApiGatewayResponse {
    private final HttpResponse httpResponse;
    private final String body;

    public GenericApiGatewayResponse(HttpResponse httpResponse) throws IOException {
        this.httpResponse = httpResponse;
        if(httpResponse.getContent() != null) {
            this.body = IOUtils.toString(httpResponse.getContent());
        }else {
            this.body = null;
        }
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public String getBody() {
        return body;
    }
}