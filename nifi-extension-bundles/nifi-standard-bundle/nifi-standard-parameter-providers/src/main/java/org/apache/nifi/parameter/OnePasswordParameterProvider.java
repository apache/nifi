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
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"1Password"})
@CapabilityDescription("Fetches parameters from 1Password Connect Server")
public class OnePasswordParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider  {

    public static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("Web Client Service Provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECT_SERVER = new PropertyDescriptor.Builder()
            .name("Connect Server")
            .displayName("Connect Server")
            .description("HTTP endpoint of the 1Password Connect Server to connect to. Example: http://localhost:8080")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .displayName("Access Token")
            .description("Access Token used for authentication against the 1Password APIs.")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final String VERSION = "v1";
    private static final String GET_VAULTS = "vaults";
    private static final String GET_ITEMS = "items";
    private static final String CONTENT_TYPE = "Content-type";
    private static final String APPLICATION_JSON = "application/json";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String AUTHORIZATION_HEADER_VALUE = "Bearer ";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            WEB_CLIENT_SERVICE_PROVIDER,
            CONNECT_SERVER,
            ACCESS_TOKEN
    );

    private volatile WebClientServiceProvider webClientServiceProvider;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final String connectServer = context.getProperty(CONNECT_SERVER).getValue();
        final URI uri = URI.create(connectServer);
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();

        try {
            final JsonNode vaultList = getVaultList(uri, accessToken);

            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Listing Vaults")
                    .explanation(String.format("Listed Vaults [%d]", vaultList.size()))
                    .build());
        } catch (final IllegalArgumentException | IOException e) {
            verificationLogger.error("Listing Vaults failed", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Listing Vaults")
                    .explanation("Listing Vaults failed: " + e.getMessage())
                    .build());
        }
        return results;
    }

    private JsonNode getVaultList(final URI connectServer, final String accessToken) throws IOException {
        try (final HttpResponseEntity getVaultList = webClientServiceProvider.getWebClientService()
                .get()
                .uri(getURI(connectServer, null, null))
                .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .retrieve()
        ) {
            return OBJECT_MAPPER.readTree(getVaultList.body());
        }
    }

    private JsonNode getItemList(final URI connectServer, final String accessToken, final String vaultID) throws IOException {
        try (final HttpResponseEntity getVaultItems = webClientServiceProvider.getWebClientService()
                .get()
                .uri(getURI(connectServer, vaultID, null))
                .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .retrieve()
        ) {
            return OBJECT_MAPPER.readTree(getVaultItems.body());
        }
    }

    private JsonNode getItemDetails(final URI connectServer, final String accessToken, final String vaultID, final String itemID) throws IOException {
        try (final HttpResponseEntity getItemDetails = webClientServiceProvider.getWebClientService()
                .get()
                .uri(getURI(connectServer, vaultID, itemID))
                .header(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE + accessToken)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .retrieve()
        ) {
            return OBJECT_MAPPER.readTree(getItemDetails.body());
        }
    }

    private URI getURI(final URI connectServer, final String vaultID, final String itemID) {
        final HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder()
                .scheme(connectServer.getScheme())
                .host(connectServer.getHost())
                .port(connectServer.getPort())
                .addPathSegment(VERSION)
                .addPathSegment(GET_VAULTS);

        if (vaultID != null) {
            uriBuilder.addPathSegment(vaultID)
            .addPathSegment(GET_ITEMS);
        }

        if (itemID != null) {
            uriBuilder.addPathSegment(itemID);
        }

        return uriBuilder.build();
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final List<ParameterGroup> parameterGroups = new ArrayList<>();

        final String connectServer = context.getProperty(CONNECT_SERVER).getValue();
        final URI uri = URI.create(connectServer);
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();

        try {
            final JsonNode vaultList = getVaultList(uri, accessToken);
            final Iterator<JsonNode> vaultIterator = vaultList.elements();

            // we iterate though each vault
            while (vaultIterator.hasNext()) {
                final JsonNode vault = vaultIterator.next();
                final String vaultName = vault.get("name").asText();
                final String vaultID = vault.get("id").asText();

                final List<Parameter> parameters = new ArrayList<>();

                final JsonNode itemList = getItemList(uri, accessToken, vaultID);
                final Iterator<JsonNode> itemIterator = itemList.elements();

                // we iterate though the items
                while (itemIterator.hasNext()) {
                    final JsonNode item = itemIterator.next();
                    final String itemID = item.get("id").asText();
                    final JsonNode itemDetails = getItemDetails(uri, accessToken, vaultID, itemID);
                    final String itemName = itemDetails.get("title").asText();
                    final Iterator<JsonNode> itemFields = itemDetails.get("fields").elements();

                    // we iterate through the fields
                    while (itemFields.hasNext()) {
                        final JsonNode field = itemFields.next();
                        final String fieldId = field.get("id").asText();
                        final JsonNode fieldValue = field.get("value");

                        if (fieldValue != null) {
                            parameters.add(new Parameter.Builder()
                                .name(itemName + "_" + fieldId)
                                .value(fieldValue.asText())
                                .provided(true)
                                .build());
                        }
                    }
                }

                parameterGroups.add(new ParameterGroup(vaultName, parameters));
            }

        } catch (final IllegalArgumentException | IOException e) {
            throw new RuntimeException("Failed to retrieve items for one or more Vaults", e);
        }

        final AtomicInteger groupedParameterCount = new AtomicInteger(0);
        final Collection<String> groupNames = new HashSet<>();
        parameterGroups.forEach(group -> {
            groupedParameterCount.addAndGet(group.getParameters().size());
            groupNames.add(group.getGroupName());
        });
        getLogger().info("Fetched {} parameters with Group Names: {}", groupedParameterCount.get(), groupNames);
        return parameterGroups;
    }
}
