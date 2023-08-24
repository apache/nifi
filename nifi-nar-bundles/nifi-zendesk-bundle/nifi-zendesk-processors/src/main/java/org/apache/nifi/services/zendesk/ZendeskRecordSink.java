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
package org.apache.nifi.services.zendesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.commons.zendesk.ZendeskAuthenticationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.HTTPS;
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

@Tags({"zendesk", "record", "sink"})
@CapabilityDescription("Create Zendesk tickets using the Zendesk API." +
        "The service requires a Zendesk account with configured access.")
public class ZendeskRecordSink extends AbstractControllerService implements RecordSinkService {

    private final ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> dynamicProperties;
    private BaseSinkZendeskTicket baseZendeskTicket;
    private ZendeskAuthenticationContext authenticationContext;
    private volatile RecordSetWriterFactory writerFactory;

    public static final PropertyDescriptor ZENDESK_TICKET_COMMENT_BODY = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_COMMENT_BODY_NAME)
            .displayName("Comment Body")
            .description("The content or the path to the comment body in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_SUBJECT = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_SUBJECT_NAME)
            .displayName("Subject")
            .description("The content or the path to the subject in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_PRIORITY = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_PRIORITY_NAME)
            .displayName("Priority")
            .description("The content or the path to the priority in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ZENDESK_TICKET_TYPE = new PropertyDescriptor.Builder()
            .name(ZENDESK_TICKET_TYPE_NAME)
            .displayName("Type")
            .description("The content or the path to the type in the incoming record.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_WRITER_FACTORY,
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
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        final WriteResult writeResult;
        List<ObjectNode> zendeskTickets = new ArrayList<>();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), out, attributes)) {

                writer.beginRecordSet();
                Record record;
                while ((record = recordSet.next()) != null) {
                    ObjectNode baseTicketNode = mapper.createObjectNode();

                    resolveFieldValue("/comment/body", baseZendeskTicket.getCommentBody(), baseTicketNode, record);

                    if (baseZendeskTicket.getSubject() != null) {
                        resolveFieldValue("/subject", baseZendeskTicket.getSubject(), baseTicketNode, record);
                    }

                    if (baseZendeskTicket.getPriority() != null) {
                        resolveFieldValue("/priority", baseZendeskTicket.getPriority(), baseTicketNode, record);
                    }
                    if (baseZendeskTicket.getType() != null) {
                        resolveFieldValue("/type", baseZendeskTicket.getType(), baseTicketNode, record);
                    }

                    for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
                        addDynamicField(dynamicProperty.getKey(), dynamicProperty.getValue(), baseTicketNode, record);
                    }

                    zendeskTickets.add(baseTicketNode);
                    writer.write(record);
                    writer.flush();
                }
                writeResult = writer.finishRecordSet();
                writer.flush();
            }
        } catch (SchemaNotFoundException e) {
            final String errorMessage = String.format("RecordSetWriter could not be created because the schema was not found. The schema name for the RecordSet to write is %s",
                    recordSet.getSchema().getSchemaName());
            throw new ProcessException(errorMessage, e);
        }

        if (zendeskTickets.size() > 0) {
            try {
                final InputStream inputStream = createRequestObject(mapper, zendeskTickets);
                final URI uri = createUri(zendeskTickets.size());
                final HttpResponseEntity response = performPostRequest(authenticationContext, webClientServiceProvider, uri, inputStream);

                if (response.statusCode() != CREATED.getCode() && response.statusCode() != OK.getCode()) {
                    getLogger().error("Failed to create zendesk ticket, HTTP status={}, response={}", response.statusCode(), responseBodyToString(response));
                }
            } catch (IOException e) {
                throw new IOException("Failed to post request to Zendesk", e);
            }
        }

        return writeResult;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        baseZendeskTicket = new BaseSinkZendeskTicket(context);
        authenticationContext = new ZendeskAuthenticationContext(context);
        dynamicProperties = getDynamicProperties(context, context.getProperties());
    }

    private URI createUri(int numberOfTickets) {
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
