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
package org.apache.nifi.processors.shopify;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.processors.shopify.model.ResourceType;
import org.apache.nifi.processors.shopify.model.ShopifyResource;
import org.apache.nifi.processors.shopify.rest.ShopifyRestService;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"shopify"})
@Stateful(scopes = Scope.CLUSTER, description =
        "For a few resources the processor supports incremental loading. The list of the resources with the supported parameters" +
                " can be found in the additional details.")
@CapabilityDescription("Retrieves object from a custom Shopify store. The processor yield time must be set to the account's rate limit accordingly.")
public class GetShopify extends AbstractProcessor {

    static final PropertyDescriptor STORE_DOMAIN = new PropertyDescriptor.Builder()
            .name("store-domain")
            .displayName("Store Domain")
            .description("The domain of the Shopify store, e.g. nifistore.myshopify.com")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Access Token to authenticate requests")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("api-version")
            .displayName("API Version")
            .description("The Shopify REST API version")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("2022-10")
            .build();

    static final PropertyDescriptor OBJECT_CATEGORY = new PropertyDescriptor.Builder()
            .name("object-category")
            .displayName("Object Category")
            .description("Shopify object category")
            .required(true)
            .allowableValues(ResourceType.class)
            .build();

    static final PropertyDescriptor RESULT_LIMIT = new PropertyDescriptor.Builder()
            .name("result-limit")
            .displayName("Result Limit")
            .description("The maximum number of results to request for each invocation of the Processor")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .build();

    static final PropertyDescriptor IS_INCREMENTAL = new PropertyDescriptor.Builder()
            .name("is-incremental")
            .displayName("Incremental Loading")
            .description("The processor can incrementally load the queried objects so that each object is queried exactly once." +
                    " For each query, the processor queries objects which were created or modified after the previous run time" +
                    " but before the current time.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor INCREMENTAL_DELAY = new PropertyDescriptor.Builder()
            .name("incremental-delay")
            .displayName("Incremental Delay")
            .description("The ending timestamp of the time window will be adjusted earlier by the amount configured in this property." +
                    " For example, with a property value of 10 seconds, an ending timestamp of 12:30:45 would be changed to 12:30:35.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
            .build();

    static final PropertyDescriptor INITIAL_INCREMENTAL_START_TIME = new PropertyDescriptor.Builder()
            .name("initial-incremental-start-time")
            .displayName("Initial Incremental Start Time")
            .description("This property specifies the start time when running the first request." +
                    " Represents an ISO 8601-encoded date and time string. For example, 3:50 pm on September 7, 2019" +
                    " in the time zone of UTC (Coordinated Universal Time) is represented as \"2019-09-07T15:50:00Z\".")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
            .build();

    public static final PropertyDescriptor WEB_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final Map<ResourceType, PropertyDescriptor> propertyMap = new EnumMap<>(ResourceType.class);
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = createPropertyDescriptors();
    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static List<PropertyDescriptor> createPropertyDescriptors() {
        final List<PropertyDescriptor> resourceDescriptors = Arrays.stream(ResourceType.values())
                .map(resourceType -> {
                    final PropertyDescriptor resourceDescriptor = new PropertyDescriptor.Builder()
                            .name(resourceType.getValue())
                            .displayName(resourceType.getPropertyDisplayName())
                            .description(resourceType.getPropertyDescription())
                            .required(true)
                            .dependsOn(OBJECT_CATEGORY, resourceType.getValue())
                            .allowableValues(resourceType.getResourcesAsAllowableValues())
                            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                            .build();
                    propertyMap.put(resourceType, resourceDescriptor);
                    return resourceDescriptor;
                })
                .collect(Collectors.toList());
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(Arrays.asList(
                STORE_DOMAIN,
                ACCESS_TOKEN,
                API_VERSION,
                OBJECT_CATEGORY
        ));
        propertyDescriptors.addAll(resourceDescriptors);
        propertyDescriptors.addAll(Arrays.asList(
                RESULT_LIMIT,
                IS_INCREMENTAL,
                INCREMENTAL_DELAY,
                INITIAL_INCREMENTAL_START_TIME,
                WEB_CLIENT_PROVIDER)
        );
        return Collections.unmodifiableList(propertyDescriptors);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();
    private static final int TOO_MANY_REQUESTS = 429;
    private static final Pattern CURSOR_PATTERN = Pattern.compile("<([^<]*)>; rel=\"next\"");

    private volatile ShopifyRestService shopifyRestService;
    private volatile ShopifyResource shopifyResource;
    private volatile String resourceName;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final WebClientServiceProvider webClientServiceProvider =
                context.getProperty(WEB_CLIENT_PROVIDER).asControllerService(WebClientServiceProvider.class);
        final WebClientService webClientService = webClientServiceProvider.getWebClientService();
        final HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder();

        final String apiVersion = context.getProperty(API_VERSION).getValue();
        final String baseUrl = context.getProperty(STORE_DOMAIN).getValue();
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();

        final String category = context.getProperty(OBJECT_CATEGORY).getValue();
        final ResourceType resourceType = ResourceType.valueOf(category);
        resourceName = context.getProperty(propertyMap.get(resourceType)).getValue();
        final String limit = context.getProperty(RESULT_LIMIT).getValue();

        shopifyResource = resourceType.getResource(resourceName);

        shopifyRestService = getShopifyRestService(webClientService, uriBuilder, apiVersion, baseUrl, accessToken, resourceName,
                limit, shopifyResource.getIncrementalLoadingParameter());
    }

    ShopifyRestService getShopifyRestService(final WebClientService webClientService, final HttpUriBuilder uriBuilder,
                                             final String apiVersion, final String baseUrl, final String accessToken, final String resourceName,
                                             final String limit, final IncrementalLoadingParameter incrementalLoadingParameter) {
        return new ShopifyRestService(
                webClientService,
                uriBuilder,
                apiVersion,
                baseUrl,
                accessToken,
                resourceName,
                limit,
                incrementalLoadingParameter
        );
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final StateMap state = getState(context);
        final boolean isIncremental = context.getProperty(IS_INCREMENTAL).asBoolean();
        final String initialStartTime = context.getProperty(INITIAL_INCREMENTAL_START_TIME).getValue();
        final Long incrDelayMs = context.getProperty(INCREMENTAL_DELAY).asTimePeriod(TimeUnit.MILLISECONDS);

        String lastExecutionTime = state.get(resourceName);
        if (lastExecutionTime == null && initialStartTime != null) {
            lastExecutionTime = initialStartTime;
        }

        Instant now = getCurrentExecutionTime();
        if (incrDelayMs != null) {
            now = now.minus(incrDelayMs, ChronoUnit.MILLIS);
        }
        final String currentExecutionTime = now.toString();

        String cursor = null;
        do {
            final AtomicInteger objectCountHolder = new AtomicInteger();
            try (HttpResponseEntity response = shopifyRestService.getShopifyObjects(isIncremental, lastExecutionTime, currentExecutionTime, cursor)) {
                cursor = getPageCursor(response);
                if (response.statusCode() == HttpResponseStatus.OK.getCode()) {
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, parseHttpResponse(response, objectCountHolder));
                    if (objectCountHolder.get() > 0) {
                        session.transfer(flowFile, REL_SUCCESS);
                        if (cursor == null && isIncremental) {
                            final Map<String, String> stateMap = new HashMap<>(state.toMap());
                            stateMap.put(resourceName, currentExecutionTime);
                            updateState(context, stateMap);
                        }
                    } else {
                        getLogger().debug("Empty response when requested Shopify resource: [{}]", resourceName);
                        session.remove(flowFile);
                    }
                } else if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    if (response.statusCode() == TOO_MANY_REQUESTS) {
                        context.yield();
                        throw new ProcessException(String.format(
                                "Rate limit exceeded, yielding before retrying request. HTTP %d error for requested URI [%s]",
                                response.statusCode(), resourceName));
                    } else {
                        context.yield();
                        getLogger().warn("HTTP {} error for requested Shopify resource [{}]", response.statusCode(),
                                resourceName);
                    }
                }
            } catch (URISyntaxException | IOException e) {
                e.printStackTrace();
            }
        } while (cursor != null);
    }

    private String getPageCursor(HttpResponseEntity response) {
        Optional<String> link = response.headers().getFirstHeader("Link");
        String s = null;
        if (link.isPresent()) {
            final Matcher matcher = CURSOR_PATTERN.matcher(link.get());
            while (matcher.find()) {
                s = matcher.group(1);
            }
        }
        return s;
    }

    private OutputStreamCallback parseHttpResponse(final HttpResponseEntity response,
                                                   final AtomicInteger objectCountHolder) {
        return out -> {
            try (JsonParser jsonParser = JSON_FACTORY.createParser(response.body());
                 final JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.getCurrentName()
                            .equals(resourceName)) {
                        jsonParser.nextToken();
                        jsonGenerator.writeStartArray();
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            jsonGenerator.copyCurrentStructure(jsonParser);
                            objectCountHolder.incrementAndGet();
                        }
                        jsonGenerator.writeEndArray();
                    }
                }
            }
        };
    }

    Instant getCurrentExecutionTime() {
        return Instant.now();
    }

    private StateMap getState(final ProcessContext context) {
        StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return state;
    }

    private void updateState(ProcessContext context, Map<String, String> newState) {
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State update failed", e);
        }
    }
}
