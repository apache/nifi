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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
@SupportsBatching
@Tags({ "elasticsearch", "query", "read", "get", "http" })
@CapabilityDescription("Queries Elasticsearch using the specified connection properties. "
        + "Note that the full body of each page of documents will be read into memory before being "
        + "written to Flow Files for transfer.  Also note that the Elasticsearch max_result_window index "
        + "setting is the upper bound on the number of records that can be retrieved using this query.  "
        + "To retrieve more records, use the ScrollElasticsearchHttp processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename attribute is set to the document identifier"),
        @WritesAttribute(attribute = "es.id", description = "The Elasticsearch document identifier"),
        @WritesAttribute(attribute = "es.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "es.type", description = "The Elasticsearch document type"),
        @WritesAttribute(attribute = "es.result.*", description = "If Target is 'Flow file attributes', the JSON attributes of "
                + "each result will be placed into corresponding attributes with this prefix.") })
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
public class QueryElasticsearchHttp extends AbstractElasticsearchHttpProcessor {

    private static final String FROM_QUERY_PARAM = "from";

    public static final String TARGET_FLOW_FILE_CONTENT = "Flow file content";
    public static final String TARGET_FLOW_FILE_ATTRIBUTES = "Flow file attributes";
    private static final String ATTRIBUTE_PREFIX = "es.result.";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                    "All FlowFiles that are read from Elasticsearch are routed to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "All FlowFiles that cannot be read from Elasticsearch are routed to this relationship. Note that only incoming "
                            + "flow files will be routed to failure.").build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description(
                    "A FlowFile is routed to this relationship if the document cannot be fetched but attempting the operation again may "
                            + "succeed. Note that if the processor has no incoming connections, flow files may still be sent to this relationship "
                            + "based on the processor properties and the results of the fetch operation.")
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("query-es-query").displayName("Query")
            .description("The Lucene-style query to run against ElasticSearch (e.g., genre:blues AND -artist:muddy)").required(true)
            .expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("query-es-index").displayName("Index")
            .description("The name of the index to read from. If the property is set "
                            + "to _all, the query will match across all indexes.").required(true)
            .expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("query-es-type")
            .displayName("Type")
            .description(
                    "The (optional) type of this query, used by Elasticsearch for indexing and searching. If the property is empty, "
                            + "the the query will match across all types.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("query-es-fields")
            .displayName("Fields")
            .description(
                    "A comma-separated list of fields to retrieve from the document. If the Fields property is left blank, "
                            + "then the entire document's source will be retrieved.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("query-es-sort")
            .displayName("Sort")
            .description(
                    "A sort parameter (e.g., timestamp:asc). If the Sort property is left blank, "
                            + "then the results will be retrieved in document order.")
            .required(false).expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("query-es-size").displayName("Page Size").defaultValue("20")
            .description("Determines how many documents to return per page during scrolling.")
            .required(true).expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    public static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("query-es-limit").displayName("Limit")
            .description("If set, limits the number of results that will be returned.")
            .required(false).expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    public static final PropertyDescriptor TARGET = new PropertyDescriptor.Builder()
            .name("query-es-target")
            .displayName("Target")
            .description(
                    "Indicates where the results should be placed.  In the case of 'Flow file content', the JSON "
                            + "response will be written as the content of the flow file.  In the case of 'Flow file attributes', "
                            + "the original flow file (if applicable) will be cloned for each result, and all return fields will be placed "
                            + "in a flow file attribute of the same name, but prefixed by 'es.result.'")
            .required(true).expressionLanguageSupported(false)
            .defaultValue(TARGET_FLOW_FILE_CONTENT)
            .allowableValues(TARGET_FLOW_FILE_CONTENT, TARGET_FLOW_FILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ES_URL);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(RESPONSE_TIMEOUT);
        descriptors.add(QUERY);
        descriptors.add(PAGE_SIZE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(FIELDS);
        descriptors.add(SORT);
        descriptors.add(LIMIT);
        descriptors.add(TARGET);

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
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {

        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can
            // continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors,
            // then
            // we know that we should run only if we have a FlowFile.
            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        OkHttpClient okHttpClient = getClient();

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile)
                .getValue();
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile)
                .getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile)
                .getValue();
        final int pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions(flowFile)
                .asInteger().intValue();
        final Integer limit = context.getProperty(LIMIT).isSet() ? context.getProperty(LIMIT)
                .evaluateAttributeExpressions(flowFile).asInteger().intValue() : null;
        final String fields = context.getProperty(FIELDS).isSet() ? context.getProperty(FIELDS)
                .evaluateAttributeExpressions(flowFile).getValue() : null;
        final String sort = context.getProperty(SORT).isSet() ? context.getProperty(SORT)
                .evaluateAttributeExpressions(flowFile).getValue() : null;
        final boolean targetIsContent = context.getProperty(TARGET).getValue()
                .equals(TARGET_FLOW_FILE_CONTENT);

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        final ComponentLog logger = getLogger();

        int fromIndex = 0;
        int numResults;

        try {
            logger.debug("Querying {}/{} from Elasticsearch: {}", new Object[] { index, docType,
                    query });

            final long startNanos = System.nanoTime();
            // read the url property from the context
            final String urlstr = StringUtils.trimToEmpty(context.getProperty(ES_URL).evaluateAttributeExpressions().getValue());

            boolean hitLimit = false;
            do {
                int mPageSize = pageSize;
                if (limit != null && limit <= (fromIndex + pageSize)) {
                    mPageSize = limit - fromIndex;
                    hitLimit = true;
                }

                final URL queryUrl = buildRequestURL(urlstr, query, index, docType, fields, sort,
                        mPageSize, fromIndex, context);

                final Response getResponse = sendRequestToElasticsearch(okHttpClient, queryUrl,
                        username, password, "GET", null);
                numResults = this.getPage(getResponse, queryUrl, context, session, flowFile,
                        logger, startNanos, targetIsContent);
                fromIndex += pageSize;
                getResponse.close();
            } while (numResults > 0 && !hitLimit);

            if (flowFile != null) {
                session.remove(flowFile);
            }
        } catch (IOException ioe) {
            logger.error(
                    "Failed to read from Elasticsearch due to {}, this may indicate an error in configuration "
                            + "(hosts, username/password, etc.). Routing to retry",
                    new Object[] { ioe.getLocalizedMessage() }, ioe);
            if (flowFile != null) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();

        } catch (RetryableException e) {
            logger.error(e.getMessage(), new Object[] { e.getLocalizedMessage() }, e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_RETRY);
            }
            context.yield();
        } catch (Exception e) {
            logger.error("Failed to read {} from Elasticsearch due to {}", new Object[] { flowFile,
                    e.getLocalizedMessage() }, e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        }
    }

    private int getPage(final Response getResponse, final URL url, final ProcessContext context,
            final ProcessSession session, FlowFile flowFile, final ComponentLog logger,
            final long startNanos, boolean targetIsContent)
            throws IOException {
        List<FlowFile> page = new ArrayList<>();
        final int statusCode = getResponse.code();

        if (isSuccess(statusCode)) {
            ResponseBody body = getResponse.body();
            final byte[] bodyBytes = body.bytes();
            JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
            JsonNode hits = responseJson.get("hits").get("hits");

            for(int i = 0; i < hits.size(); i++) {
                JsonNode hit = hits.get(i);
                String retrievedId = hit.get("_id").asText();
                String retrievedIndex = hit.get("_index").asText();
                String retrievedType = hit.get("_type").asText();

                FlowFile documentFlowFile = null;
                if (flowFile != null) {
                    documentFlowFile = targetIsContent ? session.create(flowFile) : session.clone(flowFile);
                } else {
                    documentFlowFile = session.create();
                }

                JsonNode source = hit.get("_source");
                documentFlowFile = session.putAttribute(documentFlowFile, "es.id", retrievedId);
                documentFlowFile = session.putAttribute(documentFlowFile, "es.index", retrievedIndex);
                documentFlowFile = session.putAttribute(documentFlowFile, "es.type", retrievedType);

                if (targetIsContent) {
                    documentFlowFile = session.putAttribute(documentFlowFile, "filename", retrievedId);
                    documentFlowFile = session.putAttribute(documentFlowFile, "mime.type", "application/json");
                    documentFlowFile = session.write(documentFlowFile, out -> {
                        out.write(source.toString().getBytes());
                    });
                } else {
                    Map<String, String> attributes = new HashMap<>();
                    for(Iterator<Entry<String, JsonNode>> it = source.getFields(); it.hasNext(); ) {
                        Entry<String, JsonNode> entry = it.next();
                        attributes.put(ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue().asText());
                    }
                    documentFlowFile = session.putAllAttributes(documentFlowFile, attributes);
                }
                page.add(documentFlowFile);
            }
            logger.debug("Elasticsearch retrieved " + responseJson.size() + " documents, routing to success");

            session.transfer(page, REL_SUCCESS);
        } else {
            try {
                // 5xx -> RETRY, but a server error might last a while, so yield
                if (statusCode / 100 == 5) {
                    throw new RetryableException(String.format("Elasticsearch returned code %s with message %s, transferring flow file to retry. This is likely a server problem, yielding...",
                            statusCode, getResponse.message()));
                } else if (context.hasIncomingConnection()) {  // 1xx, 3xx, 4xx -> NO RETRY
                    throw new UnretryableException(String.format("Elasticsearch returned code %s with message %s, transferring flow file to failure",
                            statusCode, getResponse.message()));
                } else {
                    logger.warn("Elasticsearch returned code {} with message {}", new Object[]{statusCode, getResponse.message()});
                }
            } finally {
                if (!page.isEmpty()) {
                    session.remove(page);
                    page.clear();
                }
            }
        }

        // emit provenance event
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (!page.isEmpty()) {
            if (context.hasNonLoopConnection()) {
                page.forEach(f -> session.getProvenanceReporter().fetch(f, url.toExternalForm(), millis));
            } else {
                page.forEach(f -> session.getProvenanceReporter().receive(f, url.toExternalForm(), millis));
            }
        }
        return page.size();
    }

    private URL buildRequestURL(String baseUrl, String query, String index, String type, String fields,
            String sort, int pageSize, int fromIndex, ProcessContext context) throws MalformedURLException {
        if (StringUtils.isEmpty(baseUrl)) {
            throw new MalformedURLException("Base URL cannot be null");
        }
        HttpUrl.Builder builder = HttpUrl.parse(baseUrl).newBuilder();
        builder.addPathSegment((StringUtils.isEmpty(index)) ? "_all" : index);
        if (!StringUtils.isEmpty(type)) {
            builder.addPathSegment(type);
        }
        builder.addPathSegment("_search");
        builder.addQueryParameter(QUERY_QUERY_PARAM, query);
        builder.addQueryParameter(SIZE_QUERY_PARAM, String.valueOf(pageSize));
        builder.addQueryParameter(FROM_QUERY_PARAM, String.valueOf(fromIndex));
        if (!StringUtils.isEmpty(fields)) {
            String trimmedFields = Stream.of(fields.split(",")).map(String::trim).collect(Collectors.joining(","));
            builder.addQueryParameter(FIELD_INCLUDE_QUERY_PARAM, trimmedFields);
        }
        if (!StringUtils.isEmpty(sort)) {
            String trimmedFields = Stream.of(sort.split(",")).map(String::trim).collect(Collectors.joining(","));
            builder.addQueryParameter(SORT_QUERY_PARAM, trimmedFields);
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
