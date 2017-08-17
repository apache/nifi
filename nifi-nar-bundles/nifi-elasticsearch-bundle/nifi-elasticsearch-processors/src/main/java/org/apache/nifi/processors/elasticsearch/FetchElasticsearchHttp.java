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
package org.apache.nifi.processors.elasticsearch;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jackson.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "fetch", "read", "get", "http"})
@CapabilityDescription("Retrieves a document from Elasticsearch using the specified connection properties and the "
        + "identifier of the document to retrieve. Note that the full body of the document will be read into memory before being "
        + "written to a Flow File for transfer.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename attribute is set to the document identifier"),
        @WritesAttribute(attribute = "es.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "es.type", description = "The Elasticsearch document type")
})
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
public class FetchElasticsearchHttp extends AbstractElasticsearchHttpProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are read from Elasticsearch are routed to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be read from Elasticsearch are routed to this relationship. Note that only incoming "
                    + "flow files will be routed to failure.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the document cannot be fetched but attempting the operation again may "
                    + "succeed. Note that if the processor has no incoming connections, flow files may still be sent to this relationship "
                    + "based on the processor properties and the results of the fetch operation.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not found")
            .description("A FlowFile is routed to this relationship if the specified document does not exist in the Elasticsearch cluster. "
                    + "Note that if the processor has no incoming connections, flow files may still be sent to this relationship based "
                    + "on the processor properties and the results of the fetch operation.")
            .build();

    public static final PropertyDescriptor DOC_ID = new PropertyDescriptor.Builder()
            .name("fetch-es-doc-id")
            .displayName("Document Identifier")
            .description("The identifier of the document to be fetched")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("fetch-es-index")
            .displayName("Index")
            .description("The name of the index to read from.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("fetch-es-type")
            .displayName("Type")
            .description("The (optional) type of this document, used by Elasticsearch for indexing and searching. If the property is empty, "
                    + "the first document matching the identifier across all types will be retrieved.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fetch-es-fields")
            .displayName("Fields")
            .description("A comma-separated list of fields to retrieve from the document. If the Fields property is left blank, "
                    + "then the entire document's source will be retrieved.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        _rels.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ES_URL);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(RESPONSE_TIMEOUT);
        descriptors.add(DOC_ID);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(FIELDS);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        OkHttpClient okHttpClient = getClient();

        if (flowFile == null) {
            flowFile = session.create();
        }

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
        final String docId = context.getProperty(DOC_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();
        final String fields = context.getProperty(FIELDS).isSet()
                ? context.getProperty(FIELDS).evaluateAttributeExpressions(flowFile).getValue()
                : null;

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        final ComponentLog logger = getLogger();

        Response getResponse = null;

        try {
            logger.debug("Fetching {}/{}/{} from Elasticsearch", new Object[]{index, docType, docId});

            // read the url property from the context
            final String urlstr = StringUtils.trimToEmpty(context.getProperty(ES_URL).evaluateAttributeExpressions().getValue());
            final URL url = buildRequestURL(urlstr, docId, index, docType, fields, context);
            final long startNanos = System.nanoTime();

            getResponse = sendRequestToElasticsearch(okHttpClient, url, username, password, "GET", null);
            final int statusCode = getResponse.code();

            if (isSuccess(statusCode)) {
                ResponseBody body = getResponse.body();
                final byte[] bodyBytes = body.bytes();
                JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
                boolean found = responseJson.get("found").asBoolean(false);
                String retrievedIndex = responseJson.get("_index").asText();
                String retrievedType = responseJson.get("_type").asText();
                String retrievedId = responseJson.get("_id").asText();

                if (found) {
                    JsonNode source = responseJson.get("_source");
                    flowFile = session.putAttribute(flowFile, "filename", retrievedId);
                    flowFile = session.putAttribute(flowFile, "es.index", retrievedIndex);
                    flowFile = session.putAttribute(flowFile, "es.type", retrievedType);
                    if (source != null) {
                        flowFile = session.write(flowFile, out -> {
                            out.write(source.toString().getBytes());
                        });
                    }
                    logger.debug("Elasticsearch document " + retrievedId + " fetched, routing to success");

                    // emit provenance event
                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    if (context.hasNonLoopConnection()) {
                        session.getProvenanceReporter().fetch(flowFile, url.toExternalForm(), millis);
                    } else {
                        session.getProvenanceReporter().receive(flowFile, url.toExternalForm(), millis);
                    }
                    session.transfer(flowFile, REL_SUCCESS);
                } else {
                    logger.warn("Failed to read {}/{}/{} from Elasticsearch: Document not found",
                            new Object[]{index, docType, docId});

                    // We couldn't find the document, so send it to "not found"
                    session.transfer(flowFile, REL_NOT_FOUND);
                }
            } else {
                if (statusCode == 404) {
                    logger.warn("Failed to read {}/{}/{} from Elasticsearch: Document not found",
                            new Object[]{index, docType, docId});

                    // We couldn't find the document, so penalize it and send it to "not found"
                    session.transfer(flowFile, REL_NOT_FOUND);
                } else {
                    // 5xx -> RETRY, but a server error might last a while, so yield
                    if (statusCode / 100 == 5) {

                        logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to retry. This is likely a server problem, yielding...",
                                new Object[]{statusCode, getResponse.message()});
                        session.transfer(flowFile, REL_RETRY);
                        context.yield();
                    } else if (context.hasIncomingConnection()) {  // 1xx, 3xx, 4xx -> NO RETRY
                        logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to failure", new Object[]{statusCode, getResponse.message()});
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        logger.warn("Elasticsearch returned code {} with message {}", new Object[]{statusCode, getResponse.message()});
                        session.remove(flowFile);
                    }
                }
            }
        } catch (IOException ioe) {
            logger.error("Failed to read from Elasticsearch due to {}, this may indicate an error in configuration "
                            + "(hosts, username/password, etc.). Routing to retry",
                    new Object[]{ioe.getLocalizedMessage()}, ioe);
            if (context.hasIncomingConnection()) {
                session.transfer(flowFile, REL_RETRY);
            } else {
                session.remove(flowFile);
            }
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to read {} from Elasticsearch due to {}", new Object[]{flowFile, e.getLocalizedMessage()}, e);
            if (context.hasIncomingConnection()) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.remove(flowFile);
            }
            context.yield();
        } finally {
            if (getResponse != null) {
                getResponse.close();
            }
        }
    }

    private URL buildRequestURL(String baseUrl, String docId, String index, String type, String fields, ProcessContext context) throws MalformedURLException {
        if (StringUtils.isEmpty(baseUrl)) {
            throw new MalformedURLException("Base URL cannot be null");
        }
        HttpUrl.Builder builder = HttpUrl.parse(baseUrl).newBuilder();
        builder.addPathSegment(index);
        builder.addPathSegment((StringUtils.isEmpty(type)) ? "_all" : type);
        builder.addPathSegment(docId);
        if (!StringUtils.isEmpty(fields)) {
            String trimmedFields = Stream.of(fields.split(",")).map(String::trim).collect(Collectors.joining(","));
            builder.addQueryParameter(FIELD_INCLUDE_QUERY_PARAM, trimmedFields);
        }

        // Find the user-added properties and set them as query parameters on the URL
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor pd = property.getKey();
            if (pd.isDynamic()) {
                if (property.getValue() != null) {
                    builder.addQueryParameter(pd.getName(), context.getProperty(pd).evaluateAttributeExpressions().getValue());
                }
            }
        }

        return builder.build().url();
    }
}
