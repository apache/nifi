/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
