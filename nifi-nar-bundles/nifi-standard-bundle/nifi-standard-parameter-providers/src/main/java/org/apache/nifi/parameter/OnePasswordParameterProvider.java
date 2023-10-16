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
package org.apache.nifi.parameter;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"1Password"})
@CapabilityDescription("Fetches parameters from 1Password.")
public class OnePasswordParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider  {


    public static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();
    public static final PropertyDescriptor CONNECT_SERVER = new PropertyDescriptor.Builder()
            .name("connect-server")
            .displayName("1Password Connect Server")
            .description("HTTP endpoint of the 1Password Connect Server to connect to. Example: http://localhost:8080")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Access Token using for authentication against the 1Password APIs.")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;
    private volatile WebClientServiceProvider webClientServiceProvider;

    private static final String GET_VAULTS = "/v1/vaults/";
    private static final String CONTENT_TYPE = "Content-type";
    private static final String APPLICATION_JSON = "application/json";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String AUTHORIZATION_HEADER_VALUE = "Bearer ";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(WEB_CLIENT_SERVICE_PROVIDER);
        properties.add(CONNECT_SERVER);
        properties.add(ACCESS_TOKEN);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final String connectServer = context.getProperty(CONNECT_SERVER).getValue();
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();

        try {
            final HttpResponseEntity getVaultList = webClientServiceProvider.getWebClientService()
                                                            .get()
                                                            .uri(URI.create(connectServer + GET_VAULTS))
                                                            .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                                                            .header(CONTENT_TYPE, APPLICATION_JSON)
                                                            .retrieve();

            final JsonNode vaultList = OBJECT_MAPPER.readTree(getVaultList.body());

            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Listing Vault(s)")
                    .explanation(String.format("Listed %s vault(s).", vaultList.size()))
                    .build());
        } catch (final IllegalArgumentException | IOException e) {
            verificationLogger.error("Failed to list vault(s)", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Listing Vault(s)")
                    .explanation("Failed to list vault(s): " + e.getMessage())
                    .build());
        }
        return results;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final List<ParameterGroup> parameterGroups = new ArrayList<>();

        final String connectServer = context.getProperty(CONNECT_SERVER).getValue();
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();

        try {
            final HttpResponseEntity getVaultList = webClientServiceProvider.getWebClientService()
                                                            .get()
                                                            .uri(URI.create(connectServer + GET_VAULTS))
                                                            .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                                                            .header(CONTENT_TYPE, APPLICATION_JSON)
                                                            .retrieve();

            final JsonNode vaultList = OBJECT_MAPPER.readTree(getVaultList.body());
            final Iterator<JsonNode> vaultIterator = vaultList.elements();

            // we iterate though each vault
            while(vaultIterator.hasNext()) {
                final JsonNode vault = vaultIterator.next();
                final String vaultName = vault.get("name").asText();
                final String vaultID = vault.get("id").asText();

                final List<Parameter> parameters = new ArrayList<Parameter>();

                final HttpResponseEntity getVaultItems = webClientServiceProvider.getWebClientService()
                                                                .get()
                                                                .uri(URI.create(connectServer + GET_VAULTS + "/" + vaultID + "/items"))
                                                                .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                                                                .header(CONTENT_TYPE, APPLICATION_JSON)
                                                                .retrieve();

                final JsonNode itemList = OBJECT_MAPPER.readTree(getVaultItems.body());
                final Iterator<JsonNode> itemIterator = itemList.elements();

                // we iterate though the items
                while(itemIterator.hasNext()) {
                    final JsonNode item = itemIterator.next();
                    final String itemID = item.get("id").asText();

                    final HttpResponseEntity getItemDetails = webClientServiceProvider.getWebClientService()
                                                                    .get()
                                                                    .uri(URI.create(connectServer + GET_VAULTS + "/" + vaultID + "/items/" + itemID))
                                                                    .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                                                                    .header(CONTENT_TYPE, APPLICATION_JSON)
                                                                    .retrieve();

                    final JsonNode itemDetails = OBJECT_MAPPER.readTree(getItemDetails.body());
                    final String itemName = itemDetails.get("title").asText();
                    final Iterator<JsonNode> itemFields = itemDetails.get("fields").elements();

                    // we iterate through the fields
                    while(itemFields.hasNext()) {
                        final JsonNode field = itemFields.next();
                        final String fieldId = field.get("id").asText();
                        final JsonNode fieldValue = field.get("value");

                        if(fieldValue != null) {
                            final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(itemName + "_" + fieldId).build();
                            parameters.add(new Parameter(parameterDescriptor, fieldValue.asText(), null, true));
                        }

                    }

                }

                parameterGroups.add(new ParameterGroup(vaultName, parameters));
            }

        } catch (final IllegalArgumentException | IOException e) {
            throw new RuntimeException("Failed to retrieve vault's items", e);
        }

        final AtomicInteger groupedParameterCount = new AtomicInteger(0);
        final Collection<String> groupNames = new HashSet<>();
        parameterGroups.forEach(group -> {
            groupedParameterCount.addAndGet(group.getParameters().size());
            groupNames.add(group.getGroupName());
        });
        getLogger().info("Fetched {} parameters.  Group names: {}", groupedParameterCount.get(), groupNames);
        return parameterGroups;
    }
}
