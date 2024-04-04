package org.apache.nifi.processors.aws.wag.client;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.regions.Region;

public class GenericApiGatewayClientBuilder {
    private String endpoint;
    private Region region;
    private AWSCredentialsProvider credentials;
    private ClientConfiguration clientConfiguration;
    private String apiKey;
    private AmazonHttpClient httpClient;

    public GenericApiGatewayClientBuilder withEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public GenericApiGatewayClientBuilder withRegion(Region region) {
        this.region = region;
        return this;
    }

    public GenericApiGatewayClientBuilder withClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    public GenericApiGatewayClientBuilder withCredentials(AWSCredentialsProvider credentials) {
        this.credentials = credentials;
        return this;
    }

    public GenericApiGatewayClientBuilder withApiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public GenericApiGatewayClientBuilder withHttpClient(AmazonHttpClient client) {
        this.httpClient = client;
        return this;
    }

    public AWSCredentialsProvider getCredentials() {
        return credentials;
    }

    public String getApiKey() {
        return apiKey;
    }

    public AmazonHttpClient getHttpClient() {
        return httpClient;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Region getRegion() {
        return region;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public GenericApiGatewayClient build() {
        Validate.notEmpty(endpoint, "Endpoint");
        Validate.notNull(region, "Region");
        return new GenericApiGatewayClient(clientConfiguration, endpoint, region, credentials, apiKey, httpClient);
    }

}
