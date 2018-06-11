package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.http.JsonResponseHandler;
import com.amazonaws.internal.auth.DefaultSignerProvider;
import com.amazonaws.protocol.json.JsonOperationMetadata;
import com.amazonaws.protocol.json.SdkStructuredPlainJsonFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.transform.JsonErrorUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericApiGatewayClient extends AmazonWebServiceClient {
    private static final String API_GATEWAY_SERVICE_NAME = "execute-api";
    private static final String API_KEY_HEADER = "x-api-key";

    private final JsonResponseHandler<GenericApiGatewayResponse> responseHandler;
    private final HttpResponseHandler<AmazonServiceException> errorResponseHandler;
    private final AWSCredentialsProvider credentials;
    private String apiKey;
    private final AWS4Signer signer;

    GenericApiGatewayClient(ClientConfiguration clientConfiguration, String endpoint, Region region,
                            AWSCredentialsProvider credentials, String apiKey, AmazonHttpClient httpClient) {
        super(clientConfiguration);
        setRegion(region);
        setEndpoint(endpoint);
        this.credentials = credentials;
        this.apiKey = apiKey;
        this.signer = new AWS4Signer();
        this.signer.setServiceName(API_GATEWAY_SERVICE_NAME);
        this.signer.setRegionName(region.getName());

        final JsonOperationMetadata metadata = new JsonOperationMetadata().withHasStreamingSuccessResponse(false).withPayloadJson(false);
        final Unmarshaller<GenericApiGatewayResponse, JsonUnmarshallerContext> responseUnmarshaller = in -> new GenericApiGatewayResponse(in.getHttpResponse());
        this.responseHandler = SdkStructuredPlainJsonFactory.SDK_JSON_FACTORY.createResponseHandler(metadata, responseUnmarshaller);
        JsonErrorUnmarshaller defaultErrorUnmarshaller = new JsonErrorUnmarshaller(GenericApiGatewayException.class, null) {
            @Override
            public AmazonServiceException unmarshall(JsonNode jsonContent) throws Exception {
                return new GenericApiGatewayException(jsonContent.toString());
            }
        };
        this.errorResponseHandler = SdkStructuredPlainJsonFactory.SDK_JSON_FACTORY.createErrorResponseHandler(
                Collections.singletonList(defaultErrorUnmarshaller), null);

        if (httpClient != null) {
            super.client = httpClient;
        }
    }

    public GenericApiGatewayResponse execute(GenericApiGatewayRequest request) {
        return execute(request.getHttpMethod(), request.getResourcePath(), request.getHeaders(), request.getParameters(), request.getBody());
    }

    private GenericApiGatewayResponse execute(HttpMethodName method, String resourcePath, Map<String, String> headers, Map<String,List<String>> parameters, InputStream content) {
        final ExecutionContext executionContext = buildExecutionContext();

        DefaultRequest request = new DefaultRequest(API_GATEWAY_SERVICE_NAME);
        request.setHttpMethod(method);
        request.setContent(content);
        request.setEndpoint(this.endpoint);
        request.setResourcePath(resourcePath);
        request.setHeaders(buildRequestHeaders(headers, apiKey));
        if (parameters != null) {
            request.setParameters(parameters);
        }
        return this.client.execute(request, responseHandler, errorResponseHandler, executionContext).getAwsResponse();
    }

    private ExecutionContext buildExecutionContext() {
        final ExecutionContext executionContext = ExecutionContext.builder().withSignerProvider(
                new DefaultSignerProvider(this, signer)).build();
        executionContext.setCredentialsProvider(credentials);
        executionContext.setSigner(signer);
        return executionContext;
    }

    private Map<String, String> buildRequestHeaders(Map<String, String> headers, String apiKey) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        if (apiKey != null) {
            headers.put(API_KEY_HEADER, apiKey);
        }
        return headers;
    }

    public URI getEndpoint() {
        return this.endpoint;
    }

    @Override
    protected String getServiceNameIntern() {
        return API_GATEWAY_SERVICE_NAME;
    }
}


