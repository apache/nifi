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
package org.apache.nifi.processors.zendesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.commons.zendesk.ZendeskAuthenticationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.HTTPS;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.REL_FAILURE_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.REL_SUCCESS_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKETS_RESOURCE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKET_RESOURCE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_HOST_TEMPLATE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_COMMENT_BODY_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_PRIORITY_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_SUBJECT_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_TYPE_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_USER;
import static org.apache.nifi.commons.zendesk.ZendeskRecordPathUtils.addDynamicField;
import static org.apache.nifi.commons.zendesk.ZendeskRecordPathUtils.resolveFieldValue;
import static org.apache.nifi.commons.zendesk.ZendeskUtils.createRequestObject;
import static org.apache.nifi.commons.zendesk.ZendeskUtils.getDynamicProperties;
import static org.apache.nifi.commons.zendesk.ZendeskUtils.performPostRequest;
import static org.apache.nifi.commons.zendesk.ZendeskUtils.responseBodyToString;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.web.client.api.HttpResponseStatus.CREATED;
import static org.apache.nifi.web.client.api.HttpResponseStatus.OK;

@Tags({"zendesk, ticket"})
@CapabilityDescription("Create Zendesk tickets using the Zendesk API.")
@DynamicProperty(
        name = "The path in the request object to add.",
        value = "The path in the incoming record to get the value from.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Additional property to be added to the Zendesk request object.")
public class PutZendeskTicket extends AbstractProcessor {

    static final String ZENDESK_RECORD_READER_NAME = "zendesk-record-reader";

    private static final ObjectMapper mapper = new ObjectMapper();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name(ZENDESK_RECORD_READER_NAME)
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_COMMENT_BODY = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_COMMENT_BODY_NAME)
            .displayName("Comment Body")
            .description("The content or the path to the comment body in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(RECORD_READER)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_SUBJECT = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_SUBJECT_NAME)
            .displayName("Subject")
            .description("The content or the path to the subject in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(RECORD_READER)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_PRIORITY = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_PRIORITY_NAME)
            .displayName("Priority")
            .description("The content or the path to the priority in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(RECORD_READER)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_TYPE = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_TYPE_NAME)
            .displayName("Type")
            .description("The content or the path to the type in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(RECORD_READER)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            WEB_CLIENT_SERVICE_PROVIDER,
            ZENDESK_SUBDOMAIN,
            ZENDESK_USER,
            ZENDESK_AUTHENTICATION_TYPE,
            ZENDESK_AUTHENTICATION_CREDENTIAL,
            ZENDESK_TICKET_COMMENT_BODY,
            ZENDESK_TICKET_SUBJECT,
            ZENDESK_TICKET_PRIORITY,
            ZENDESK_TICKET_TYPE
    ));

    private volatile WebClientServiceProvider webClientServiceProvider;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name(REL_SUCCESS_NAME)
            .description("For FlowFiles created as a result of a successful HTTP request.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name(REL_FAILURE_NAME)
            .description("A FlowFile is routed to this relationship if the operation failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        long startNanos = System.nanoTime();
        HttpResponseEntity response;
        URI uri;
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final ZendeskAuthenticationContext authenticationContext = new ZendeskAuthenticationContext(context);

        if (readerFactory == null) {
            try (final InputStream inputStream = session.read(flowFile)) {
                final HttpUriBuilder uriBuilder = uriBuilder(authenticationContext.getSubDomain(), ZENDESK_CREATE_TICKET_RESOURCE);
                uri = uriBuilder.build();
                response = performPostRequest(authenticationContext, webClientServiceProvider, uri, inputStream);
            } catch (IOException e) {
                throw new ProcessException("Could not read incoming FlowFile", e);
            }
        } else {
            final String commentBody = context.getProperty(ZENDESK_TICKET_COMMENT_BODY).evaluateAttributeExpressions().getValue();
            final String subject = context.getProperty(ZENDESK_TICKET_SUBJECT).evaluateAttributeExpressions().getValue();
            final String priority = context.getProperty(ZENDESK_TICKET_PRIORITY).evaluateAttributeExpressions().getValue();
            final String type = context.getProperty(ZENDESK_TICKET_TYPE).evaluateAttributeExpressions().getValue();
            final Map<String, String> dynamicProperties = getDynamicProperties(context, context.getProperties());
            List<ObjectNode> zendeskTickets = new ArrayList<>();

            try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                Record record;

                while ((record = reader.nextRecord()) != null) {
                    ObjectNode baseTicketNode = mapper.createObjectNode();
                    resolveFieldValue("/comment/body", commentBody, baseTicketNode, record);

                    if (subject != null) {
                        resolveFieldValue("/subject", subject, baseTicketNode, record);
                    }
                    if (priority != null) {
                        resolveFieldValue("/priority", priority, baseTicketNode, record);
                    }
                    if (type != null) {
                        resolveFieldValue("/type", type, baseTicketNode, record);
                    }
                    for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
                        addDynamicField(dynamicProperty.getKey(), dynamicProperty.getValue(), baseTicketNode, record);
                    }
                    zendeskTickets.add(baseTicketNode);
                }

            } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
                getLogger().error("Error occurred while creating zendesk tickets", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            if (zendeskTickets.size() == 0) {
                getLogger().info("No record were received");
                return;
            }

            try {
                final InputStream inputStream = createRequestObject(mapper, zendeskTickets);
                uri = createUri(authenticationContext, zendeskTickets.size());
                response = performPostRequest(authenticationContext, webClientServiceProvider, uri, inputStream);
            } catch (IOException e) {
                getLogger().error("Failed to post request to Zendesk", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
        }

        handleResponse(session, flowFile, response, uri, startNanos);
    }

    private void handleResponse(ProcessSession session, FlowFile flowFile, HttpResponseEntity response, URI uri, long startNanos) {
        if (response.statusCode() == CREATED.getCode() || response.statusCode() == OK.getCode()) {
            session.transfer(flowFile, REL_SUCCESS);
            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, uri.toString(), transferMillis);
        } else {
            getLogger().error("Zendesk ticket creation returned with error, HTTP status={}, response={}", response.statusCode(), responseBodyToString(response));
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    private URI createUri(ZendeskAuthenticationContext authenticationContext, int numberOfTickets) {
        final String resource = numberOfTickets > 1 ? ZENDESK_CREATE_TICKETS_RESOURCE : ZENDESK_CREATE_TICKET_RESOURCE;
        final HttpUriBuilder uriBuilder = uriBuilder(authenticationContext.getSubDomain(), resource);
        return uriBuilder.build();
    }

    HttpUriBuilder uriBuilder(String subDomain, String resourcePath) {
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(format(ZENDESK_HOST_TEMPLATE, subDomain))
                .encodedPath(resourcePath);
    }
}
