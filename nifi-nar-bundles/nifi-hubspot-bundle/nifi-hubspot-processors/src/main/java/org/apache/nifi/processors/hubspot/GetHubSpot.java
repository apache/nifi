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
package org.apache.nifi.processors.hubspot;

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
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hubspot"})
@CapabilityDescription("Retrieves JSON data from a private HubSpot application."
        + " Supports incremental retrieval: Users can set the \"limit\" property which serves as the upper limit of the retrieved objects."
        + " When this property is set the processor will retrieve new records. This processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "When the 'Limit' attribute is set, the paging cursor is saved after executing a request."
        + " Only the objects after the paging cursor will be retrieved. The maximum number of retrieved objects is the 'Limit' attribute."
        + " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected,"
        + " the new node can pick up where the previous node left off, without duplicating the data.")
public class GetHubSpot extends AbstractProcessor {

    // OBJECTS

    static final AllowableValue COMPANIES = new AllowableValue(
            "/crm/v3/objects/companies",
            "Companies",
            "In HubSpot, the companies object is a standard CRM object. Individual company records can be used to store information about businesses" +
                    " and organizations within company properties."
    );
    static final AllowableValue CONTACTS = new AllowableValue(
            "/crm/v3/objects/contacts",
            "Contacts",
            "In HubSpot, contacts store information about individuals. From marketing automation to smart content, the lead-specific data found in" +
                    " contact records helps users leverage much of HubSpot's functionality."
    );
    static final AllowableValue DEALS = new AllowableValue(
            "/crm/v3/objects/deals",
            "In HubSpot, a deal represents an ongoing transaction that a sales team is pursuing with a contact or company. It’s tracked through" +
                    " pipeline stages until won or lost."
    );
    static final AllowableValue FEEDBACK_SUBMISSIONS = new AllowableValue(
            "/crm/v3/objects/feedback_submissions",
            "In HubSpot, feedback submissions are an object which stores information submitted to a feedback survey. This includes Net Promoter Score (NPS)," +
                    " Customer Satisfaction (CSAT), Customer Effort Score (CES) and Custom Surveys."
    );
    static final AllowableValue LINE_ITEMS = new AllowableValue(
            "/crm/v3/objects/line_items",
            "Line Items",
            "In HubSpot, line items can be thought of as a subset of products. When a product is attached to a deal, it becomes a line item. Line items can" +
                    " be created that are unique to an individual quote, but they will not be added to the product library."
    );
    static final AllowableValue PRODUCTS = new AllowableValue(
            "/crm/v3/objects/products",
            "Products",
            "In HubSpot, products represent the goods or services to be sold. Building a product library allows the user to quickly add products to deals," +
                    " generate quotes, and report on product performance."
    );
    static final AllowableValue TICKETS = new AllowableValue(
            "/crm/v3/objects/tickets",
            "Tickets",
            "In HubSpot, a ticket represents a customer request for help or support."
    );
    static final AllowableValue QUOTES = new AllowableValue(
            "/crm/v3/objects/quotes",
            "Quotes",
            "In HubSpot, quotes are used to share pricing information with potential buyers."
    );

    // ENGAGEMENTS

    private static final AllowableValue CALLS = new AllowableValue(
            "/crm/v3/objects/calls",
            "Calls",
            "Get calls on CRM records and on the calls index page."
    );
    private static final AllowableValue EMAILS = new AllowableValue(
            "/crm/v3/objects/emails",
            "Emails",
            "Get emails on CRM records."
    );
    private static final AllowableValue MEETINGS = new AllowableValue(
            "/crm/v3/objects/meetings",
            "Meetings",
            "Get meetings on CRM records."
    );
    private static final AllowableValue NOTES = new AllowableValue(
            "/crm/v3/objects/notes",
            "Notes",
            "Get notes on CRM records."
    );
    private static final AllowableValue TASKS = new AllowableValue(
            "/crm/v3/objects/tasks",
            "Tasks",
            "Get tasks on CRM records."
    );

    // OTHER

    private static final AllowableValue OWNERS = new AllowableValue(
            "/crm/v3/owners/",
            "Owners",
            "HubSpot uses owners to assign specific users to contacts, companies, deals, tickets, or engagements. Any HubSpot user with access to contacts" +
                    " can be assigned as an owner, and multiple owners can be assigned to an object by creating a custom property for this purpose."
    );

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("hubspot-admin-api-access-token")
            .displayName("Admin API Access Token")
            .description("")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor CRM_ENDPOINT = new PropertyDescriptor.Builder()
            .name("hubspot-crm-endpoint")
            .displayName("HubSpot CRM API Endpoint")
            .description("The HubSpot CRM API endpoint to get")
            .required(true)
            .allowableValues(COMPANIES, CONTACTS, DEALS, FEEDBACK_SUBMISSIONS, LINE_ITEMS, PRODUCTS, TICKETS, QUOTES,
                    CALLS, EMAILS, MEETINGS, NOTES, TASKS, OWNERS)
            .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("hubspot-crm-limit")
            .displayName("Limit")
            .description("The maximum number of results to display per page")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor WEB_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("nifi-web-client")
            .displayName("NiFi Web Client")
            .description("Controller service for HTTP client operations")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = createPropertyDescriptors();
    public static final String API_BASE_URI = "api.hubapi.com";
    public static final String HTTPS = "https";

    private volatile WebClientServiceProvider webClientServiceProvider;

    private static List<PropertyDescriptor> createPropertyDescriptors() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(Arrays.asList(
                ACCESS_TOKEN,
                CRM_ENDPOINT,
                LIMIT,
                WEB_CLIENT_PROVIDER
        ));
        return Collections.unmodifiableList(propertyDescriptors);
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_PROVIDER).asControllerService(WebClientServiceProvider.class);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String endpoint = context.getProperty(CRM_ENDPOINT).getValue();

        final StateMap state = getStateMap(context);
        final URI uri = createUri(context, state);

        final HttpResponseEntity response = getHttpResponseEntity(accessToken, uri);
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonFactory jsonFactory = objectMapper.getFactory();
        FlowFile flowFile = session.create();

        flowFile = session.write(flowFile, out -> {


            try (JsonParser jsonParser = jsonFactory.createParser(response.body());
                 final JsonGenerator jsonGenerator = jsonFactory.createGenerator(out, JsonEncoding.UTF8)) {
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.getCurrentName().equals("results")) {
                        jsonParser.nextToken();
                        jsonGenerator.copyCurrentStructure(jsonParser);
                    }
                    String fieldname = jsonParser.getCurrentName();
                    if ("after".equals(fieldname)) {
                        jsonParser.nextToken();
                        Map<String, String> newStateMap = new HashMap<>(state.toMap());
                        newStateMap.put(endpoint, jsonParser.getText());
                        updateState(context, newStateMap);
                        break;
                    }
                }
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }

    HttpResponseEntity getHttpResponseEntity(final String accessToken, final URI uri) {
        return webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .retrieve();
    }

    HttpUriBuilder getBaseUri(final ProcessContext context) {
        final String path = context.getProperty(CRM_ENDPOINT).getValue();
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(API_BASE_URI)
                .encodedPath(path);
    }

    URI createUri(final ProcessContext context, final StateMap state) {
        final String path = context.getProperty(CRM_ENDPOINT).getValue();
        final HttpUriBuilder uriBuilder = getBaseUri(context);

        final boolean isLimitSet = context.getProperty(LIMIT).isSet();
        if (isLimitSet) {
            final String limit = context.getProperty(LIMIT).getValue();
            uriBuilder.addQueryParameter("limit", limit);
        }

        final String cursor = state.get(path);
        if (cursor != null) {
            uriBuilder.addQueryParameter("after", cursor);
        }
        return uriBuilder.build();
    }

    private StateMap getStateMap(final ProcessContext context) {
        final StateMap stateMap;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return stateMap;
    }

    private void updateState(ProcessContext context, Map<String, String> newState) {
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Page cursor update failed", e);
        }
    }
}
