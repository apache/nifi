/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.solr;

import com.google.gson.stream.JsonWriter;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.IntervalFacet;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.client.solrj.response.RangeFacet.Count;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.StatsParams;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_PRINCIPAL;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_USER_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE;
import static org.apache.nifi.processors.solr.SolrUtils.COLLECTION;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE_CLOUD;
import static org.apache.nifi.processors.solr.SolrUtils.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_SOCKET_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CLIENT_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.RECORD_WRITER;

@Tags({"Apache", "Solr", "Get", "Query", "Records"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Queries Solr and outputs the results as a FlowFile in the format of XML or using a Record Writer")
@DynamicProperty(name="A Solr request parameter name", value="A Solr request parameter value",
        description="These parameters will be passed to Solr on the request")
@WritesAttributes({
        @WritesAttribute(attribute = "solr.connect", description = "Solr connect string"),
        @WritesAttribute(attribute = "solr.collection", description = "Solr collection"),
        @WritesAttribute(attribute = "solr.query", description = "Query string sent to Solr"),
        @WritesAttribute(attribute = "solr.cursor.mark", description = "Cursor mark can be used for scrolling Solr"),
        @WritesAttribute(attribute = "solr.status.code", description = "Status code of Solr request. A status code of 0 indicates that the request was successfully processed"),
        @WritesAttribute(attribute = "solr.query.time", description = "The elapsed time to process the query (in ms)"),
        @WritesAttribute(attribute = "solr.start", description = "Solr start parameter (result offset) for the query"),
        @WritesAttribute(attribute = "solr.rows", description = "Number of Solr documents to be returned for the query"),
        @WritesAttribute(attribute = "solr.number.results", description = "Number of Solr documents that match the query"),
        @WritesAttribute(attribute = "mime.type", description = "The mime type of the data format"),
        @WritesAttribute(attribute = "querysolr.exeption.class", description = "The Java exception class raised when the processor fails"),
        @WritesAttribute(attribute = "querysolr.exeption.message", description = "The Java exception message raised when the processor fails")
})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class QuerySolr extends SolrProcessor {

    public static final AllowableValue MODE_XML = new AllowableValue("XML");
    public static final AllowableValue MODE_REC = new AllowableValue("Records");

    public static final AllowableValue RETURN_TOP_RESULTS = new AllowableValue("return_only_top_results", "Only top results");
    public static final AllowableValue RETURN_ALL_RESULTS = new AllowableValue("return_all_results", "Entire results");

    public static final String MIME_TYPE_JSON = "application/json";
    public static final String MIME_TYPE_XML = "application/xml";
    public static final String ATTRIBUTE_SOLR_CONNECT = "solr.connect";
    public static final String ATTRIBUTE_SOLR_COLLECTION = "solr.collection";
    public static final String ATTRIBUTE_SOLR_QUERY = "solr.query";
    public static final String ATTRIBUTE_CURSOR_MARK = "solr.cursor.mark";
    public static final String ATTRIBUTE_SOLR_STATUS = "solr.status.code";
    public static final String ATTRIBUTE_SOLR_START = "solr.start";
    public static final String ATTRIBUTE_SOLR_ROWS = "solr.rows";
    public static final String ATTRIBUTE_SOLR_NUMBER_RESULTS = "solr.number.results";
    public static final String ATTRIBUTE_QUERY_TIME = "solr.query.time";
    public static final String EXCEPTION = "querysolr.exeption";
    public static final String EXCEPTION_MESSAGE = "querysolr.exeption.message";

    public static final Integer UPPER_LIMIT_START_PARAM = 10000;

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor
            .Builder().name("return_type")
            .displayName("Return Type")
            .description("Output format of Solr results. Write Solr documents to FlowFiles as XML or using a Record Writer")
            .required(true)
            .allowableValues(MODE_XML, MODE_REC)
            .defaultValue(MODE_XML.getValue())
            .build();

    public static final PropertyDescriptor SOLR_PARAM_QUERY = new PropertyDescriptor
            .Builder().name("solr_param_query")
            .displayName("Solr Query")
            .description("Solr Query, e. g. field:value")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("*:*")
            .build();

    public static final PropertyDescriptor SOLR_PARAM_REQUEST_HANDLER = new PropertyDescriptor
            .Builder().name("solr_param_request_handler")
            .displayName("Request Handler")
            .description("Define a request handler here, e. g. /query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("/select")
            .build();

    public static final PropertyDescriptor SOLR_PARAM_FIELD_LIST = new PropertyDescriptor
            .Builder().name("solr_param_field_list")
            .displayName("Field List")
            .description("Comma separated list of fields to be included into results, e. g. field1,field2")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOLR_PARAM_SORT = new PropertyDescriptor
            .Builder().name("solr_param_sort")
            .displayName("Sorting of result list")
            .description("Comma separated sort clauses to define the sorting of results, e. g. field1 asc, field2 desc")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOLR_PARAM_START = new PropertyDescriptor
            .Builder().name("solr_param_start")
            .displayName("Start of results")
            .description("Offset of result set")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOLR_PARAM_ROWS = new PropertyDescriptor
            .Builder().name("solr_param_rows")
            .displayName("Rows")
            .description("Number of results to be returned for a single request")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AMOUNT_DOCUMENTS_TO_RETURN = new PropertyDescriptor
            .Builder().name("amount_documents_to_return")
            .displayName("Total amount of returned results")
            .description("Total amount of Solr documents to be returned. If this property is set to \"Only top results\", " +
                    "only single requests will be sent to Solr and the results will be written into single FlowFiles. If it is set to " +
                    "\"Entire results\", all results matching to the query are retrieved via multiple Solr requests and " +
                    "returned in multiple FlowFiles. For both options, the number of Solr documents to be returned in a FlowFile depends on " +
                    "the configuration of the \"Rows\" property")
            .required(true)
            .allowableValues(RETURN_ALL_RESULTS, RETURN_TOP_RESULTS)
            .defaultValue(RETURN_TOP_RESULTS.getValue())
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value to send for the '" + propertyDescriptorName + "' Solr parameter")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    public static final Relationship RESULTS = new Relationship.Builder().name("results")
            .description("Results of Solr queries").build();
    public static final Relationship FACETS = new Relationship.Builder().name("facets")
            .description("Results of faceted search").build();
    public static final Relationship STATS = new Relationship.Builder().name("stats")
            .description("Stats about Solr index").build();
    public static final Relationship ORIGINAL = new Relationship.Builder().name("original")
            .description("Original flowfile").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Failure relationship").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(RETURN_TYPE);
        descriptors.add(RECORD_WRITER);
        descriptors.add(SOLR_PARAM_QUERY);
        descriptors.add(SOLR_PARAM_REQUEST_HANDLER);
        descriptors.add(SOLR_PARAM_FIELD_LIST);
        descriptors.add(SOLR_PARAM_SORT);
        descriptors.add(SOLR_PARAM_START);
        descriptors.add(SOLR_PARAM_ROWS);
        descriptors.add(AMOUNT_DOCUMENTS_TO_RETURN);
        descriptors.add(KERBEROS_CREDENTIALS_SERVICE);
        descriptors.add(KERBEROS_USER_SERVICE);
        descriptors.add(KERBEROS_PRINCIPAL);
        descriptors.add(KERBEROS_PASSWORD);
        descriptors.add(BASIC_USERNAME);
        descriptors.add(BASIC_PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(SOLR_SOCKET_TIMEOUT);
        descriptors.add(SOLR_CONNECTION_TIMEOUT);
        descriptors.add(SOLR_MAX_CONNECTIONS);
        descriptors.add(SOLR_MAX_CONNECTIONS_PER_HOST);
        descriptors.add(ZK_CLIENT_TIMEOUT);
        descriptors.add(ZK_CONNECTION_TIMEOUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(FAILURE);
        relationships.add(RESULTS);
        relationships.add(FACETS);
        relationships.add(STATS);
        relationships.add(ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    public static final Set<String> SUPPORTED_SEARCH_COMPONENTS = new HashSet<>();
    static {
        SUPPORTED_SEARCH_COMPONENTS.addAll(Arrays.asList(StatsParams.STATS, FacetParams.FACET));
    }

    public static final Set<String> SEARCH_COMPONENTS_ON = new HashSet<>();
    static {
        SEARCH_COMPONENTS_ON.addAll(Arrays.asList("true", "on", "yes"));
    }

    @Override
    protected final Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final Collection<ValidationResult> problems = new ArrayList<>();

        if (context.getProperty(RETURN_TYPE).evaluateAttributeExpressions().getValue().equals(MODE_REC.getValue())
                && !context.getProperty(RECORD_WRITER).isSet()) {
            problems.add(new ValidationResult.Builder()
                    .explanation("for writing records a record writer has to be configured")
                    .valid(false)
                    .subject("Record writer check")
                    .build());
        }
        return problems;
    }

    @Override
    public void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        FlowFile flowFileOriginal = session.get();
        FlowFile flowFileResponse;

        if (flowFileOriginal == null) {
            if (context.hasNonLoopConnection()) {
                return;
            }
            flowFileResponse = session.create();
        } else {
            flowFileResponse = session.create(flowFileOriginal);
        }

        final SolrQuery solrQuery = new SolrQuery();
        final boolean isSolrCloud = SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue());
        final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions(flowFileResponse).getValue();

        final StringBuilder transitUri = new StringBuilder("solr://");
        transitUri.append(getSolrLocation());
        if (isSolrCloud) {
            transitUri.append(":").append(collection);
        }
        final StopWatch timer = new StopWatch(false);

        try {
            solrQuery.setQuery(context.getProperty(SOLR_PARAM_QUERY).evaluateAttributeExpressions(flowFileResponse).getValue());
            solrQuery.setRequestHandler(context.getProperty(SOLR_PARAM_REQUEST_HANDLER).evaluateAttributeExpressions(flowFileResponse).getValue());

            if (context.getProperty(SOLR_PARAM_FIELD_LIST).isSet()) {
                for (final String field : context.getProperty(SOLR_PARAM_FIELD_LIST).evaluateAttributeExpressions(flowFileResponse).getValue()
                        .split(",")) {
                    solrQuery.addField(field.trim());
                }
            }

            // Avoid ArrayIndexOutOfBoundsException due to incorrectly configured sorting
            try {
                if (context.getProperty(SOLR_PARAM_SORT).isSet()) {
                    final List<SolrQuery.SortClause> sortings = new ArrayList<>();
                    for (final String sorting : context.getProperty(SOLR_PARAM_SORT).evaluateAttributeExpressions(flowFileResponse).getValue()
                            .split(",")) {
                        final String[] sortEntry = sorting.trim().split(" ");
                        sortings.add(new SolrQuery.SortClause(sortEntry[0], sortEntry[1]));
                    }
                    solrQuery.setSorts(sortings);
                }
            } catch (Exception e) {
                throw new ProcessException("Error while parsing the sort clauses for the Solr query");
            }

            final Integer startParam = context.getProperty(SOLR_PARAM_START).isSet() ? Integer.parseInt(
                    context.getProperty(SOLR_PARAM_START).evaluateAttributeExpressions(flowFileResponse).getValue()) : CommonParams.START_DEFAULT;

            solrQuery.setStart(startParam);

            final Integer rowParam = context.getProperty(SOLR_PARAM_ROWS).isSet() ? Integer.parseInt(
                    context.getProperty(SOLR_PARAM_ROWS).evaluateAttributeExpressions(flowFileResponse).getValue()) : CommonParams.ROWS_DEFAULT;

            solrQuery.setRows(rowParam);

            final Map<String,String[]> additionalSolrParams = SolrUtils.getRequestParams(context, flowFileResponse);

            final Set<String> searchComponents = extractSearchComponents(additionalSolrParams);
            solrQuery.add(new MultiMapSolrParams(additionalSolrParams));

            final Map<String,String> attributes = new HashMap<>();
            attributes.put(ATTRIBUTE_SOLR_CONNECT, getSolrLocation());
            if (isSolrCloud) {
                attributes.put(ATTRIBUTE_SOLR_COLLECTION, collection);
            }
            attributes.put(ATTRIBUTE_SOLR_QUERY, solrQuery.toString());
            if (flowFileOriginal != null) {
                flowFileOriginal = session.putAllAttributes(flowFileOriginal, attributes);
            }

            flowFileResponse = session.putAllAttributes(flowFileResponse, attributes);

            final boolean getEntireResults = RETURN_ALL_RESULTS.equals(context.getProperty(AMOUNT_DOCUMENTS_TO_RETURN).getValue());
            boolean processFacetsAndStats = true;
            boolean continuePaging = true;

            while (continuePaging){

                timer.start();

                Map<String,String> responseAttributes = new HashMap<>();
                responseAttributes.put(ATTRIBUTE_SOLR_START, solrQuery.getStart().toString());
                responseAttributes.put(ATTRIBUTE_SOLR_ROWS, solrQuery.getRows().toString());

                if (solrQuery.getStart() > UPPER_LIMIT_START_PARAM) {
                    logger.warn("The start parameter of Solr query {} exceeded the upper limit of {}. The query will not be processed " +
                            "to avoid performance or memory issues on the part of Solr.", new Object[]{solrQuery.toString(), UPPER_LIMIT_START_PARAM});
                    flowFileResponse = session.putAllAttributes(flowFileResponse, responseAttributes);
                    timer.stop();
                    break;
                }

                final QueryRequest req = new QueryRequest(solrQuery);
                if (isBasicAuthEnabled()) {
                    req.setBasicAuthCredentials(getUsername(), getPassword());
                }

                final QueryResponse response = req.process(getSolrClient());
                timer.stop();

                final Long totalNumberOfResults = response.getResults().getNumFound();

                responseAttributes.put(ATTRIBUTE_SOLR_NUMBER_RESULTS, totalNumberOfResults.toString());
                responseAttributes.put(ATTRIBUTE_CURSOR_MARK, response.getNextCursorMark());
                responseAttributes.put(ATTRIBUTE_SOLR_STATUS, String.valueOf(response.getStatus()));
                responseAttributes.put(ATTRIBUTE_QUERY_TIME, String.valueOf(response.getQTime()));
                flowFileResponse = session.putAllAttributes(flowFileResponse, responseAttributes);

                if (response.getResults().size() > 0) {

                    if (context.getProperty(RETURN_TYPE).getValue().equals(MODE_XML.getValue())){
                        flowFileResponse = session.write(flowFileResponse, SolrUtils.getOutputStreamCallbackToTransformSolrResponseToXml(response));
                        flowFileResponse = session.putAttribute(flowFileResponse, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_XML);
                    } else {
                        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).evaluateAttributeExpressions(flowFileResponse)
                                .asControllerService(RecordSetWriterFactory.class);
                        final RecordSchema schema = writerFactory.getSchema(flowFileResponse.getAttributes(), null);
                        final RecordSet recordSet = SolrUtils.solrDocumentsToRecordSet(response.getResults(), schema);
                        final StringBuffer mimeType = new StringBuffer();
                        final FlowFile flowFileResponseRef = flowFileResponse;
                        flowFileResponse = session.write(flowFileResponse, out -> {
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, flowFileResponseRef)) {
                                writer.write(recordSet);
                                writer.flush();
                                mimeType.append(writer.getMimeType());
                            } catch (SchemaNotFoundException e) {
                                throw new ProcessException("Could not parse Solr response", e);
                            }
                        });
                        flowFileResponse = session.putAttribute(flowFileResponse, CoreAttributes.MIME_TYPE.key(), mimeType.toString());
                    }

                    if (processFacetsAndStats) {
                        if (searchComponents.contains(FacetParams.FACET)) {
                            FlowFile flowFileFacets = session.create(flowFileResponse);
                            flowFileFacets = session.write(flowFileFacets, out -> {
                                try (
                                        final OutputStreamWriter osw = new OutputStreamWriter(out);
                                        final JsonWriter writer = new JsonWriter(osw)
                                ) {
                                    addFacetsFromSolrResponseToJsonWriter(response, writer);
                                }
                            });
                            flowFileFacets = session.putAttribute(flowFileFacets, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_JSON);
                            session.getProvenanceReporter().receive(flowFileFacets, transitUri.toString(), timer.getDuration(TimeUnit.MILLISECONDS));
                            session.transfer(flowFileFacets, FACETS);
                        }

                        if (searchComponents.contains(StatsParams.STATS)) {
                            FlowFile flowFileStats = session.create(flowFileResponse);
                            flowFileStats = session.write(flowFileStats, out -> {
                                try (
                                        final OutputStreamWriter osw = new OutputStreamWriter(out);
                                        final JsonWriter writer = new JsonWriter(osw)
                                ) {
                                    addStatsFromSolrResponseToJsonWriter(response, writer);
                                }
                            });
                            flowFileStats = session.putAttribute(flowFileStats, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_JSON);
                            session.getProvenanceReporter().receive(flowFileStats, transitUri.toString(), timer.getDuration(TimeUnit.MILLISECONDS));
                            session.transfer(flowFileStats, STATS);
                        }
                        processFacetsAndStats = false;
                    }
                }

                if (getEntireResults) {
                    final Integer totalDocumentsReturned = solrQuery.getStart() + solrQuery.getRows();
                    if (totalDocumentsReturned < totalNumberOfResults) {
                        solrQuery.setStart(totalDocumentsReturned);
                        session.getProvenanceReporter().receive(flowFileResponse, transitUri.toString(), timer.getDuration(TimeUnit.MILLISECONDS));
                        session.transfer(flowFileResponse, RESULTS);
                        flowFileResponse = session.create(flowFileResponse);
                    } else {
                        continuePaging = false;
                    }
                } else {
                    continuePaging = false;
                }
            }

        } catch (Exception e) {
            flowFileResponse = session.penalize(flowFileResponse);
            flowFileResponse = session.putAttribute(flowFileResponse, EXCEPTION, e.getClass().getName());
            flowFileResponse = session.putAttribute(flowFileResponse, EXCEPTION_MESSAGE, e.getMessage());
            session.transfer(flowFileResponse, FAILURE);
            logger.error("Failed to execute query {} due to {}. FlowFile will be routed to relationship failure", solrQuery.toString(), e, e);
            if (flowFileOriginal != null) {
                flowFileOriginal = session.penalize(flowFileOriginal);
            }
        }

        if (!flowFileResponse.isPenalized()) {
            session.getProvenanceReporter().receive(flowFileResponse, transitUri.toString(), timer.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFileResponse, RESULTS);
        }

        if (flowFileOriginal != null) {
            if (!flowFileOriginal.isPenalized()) {
                session.transfer(flowFileOriginal, ORIGINAL);
            } else {
                session.remove(flowFileOriginal);
            }
        }
    }

    private Set<String> extractSearchComponents(Map<String,String[]> solrParams) {
        final Set<String> searchComponentsTemp = new HashSet<>();
        for (final String searchComponent : SUPPORTED_SEARCH_COMPONENTS)
            if (solrParams.keySet().contains(searchComponent)) {
                if (SEARCH_COMPONENTS_ON.contains(solrParams.get(searchComponent)[0])) {
                    searchComponentsTemp.add(searchComponent);
                }
            }
        return Collections.unmodifiableSet(searchComponentsTemp);
    }

    private static void addStatsFromSolrResponseToJsonWriter(final QueryResponse response, final JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("stats_fields");
        writer.beginObject();
        for (Map.Entry<String,FieldStatsInfo> entry: response.getFieldStatsInfo().entrySet()) {
            FieldStatsInfo fsi = entry.getValue();
            writer.name(entry.getKey());
            writer.beginObject();
            writer.name("min").value(fsi.getMin().toString());
            writer.name("max").value(fsi.getMax().toString());
            writer.name("count").value(fsi.getCount());
            writer.name("missing").value(fsi.getMissing());
            writer.name("sum").value(fsi.getSum().toString());
            writer.name("mean").value(fsi.getMean().toString());
            writer.name("sumOfSquares").value(fsi.getSumOfSquares());
            writer.name("stddev").value(fsi.getStddev());
            writer.endObject();
        }
        writer.endObject();
        writer.endObject();
    }

    private static void addFacetsFromSolrResponseToJsonWriter(final QueryResponse response, final JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("facet_queries");
        writer.beginArray();
        for (final Map.Entry<String,Integer> facetQuery : response.getFacetQuery().entrySet()){
            writer.beginObject();
            writer.name("facet").value(facetQuery.getKey());
            writer.name("count").value(facetQuery.getValue());
            writer.endObject();
        }
        writer.endArray();

        writer.name("facet_fields");
        writer.beginObject();
        for (final FacetField facetField : response.getFacetFields()){
            writer.name(facetField.getName());
            writer.beginArray();
            for (final FacetField.Count count : facetField.getValues()) {
                writer.beginObject();
                writer.name("facet").value(count.getName());
                writer.name("count").value(count.getCount());
                writer.endObject();
            }
            writer.endArray();
        }
        writer.endObject();

        writer.name("facet_ranges");
        writer.beginObject();
        for (final RangeFacet rangeFacet : response.getFacetRanges()) {
            writer.name(rangeFacet.getName());
            writer.beginArray();
            final List<Count> list = rangeFacet.getCounts();
            for (final Count count : list) {
                writer.beginObject();
                writer.name("facet").value(count.getValue());
                writer.name("count").value(count.getCount());
                writer.endObject();
            }
            writer.endArray();
        }
        writer.endObject();

        writer.name("facet_intervals");
        writer.beginObject();
        for (final IntervalFacet intervalFacet : response.getIntervalFacets()) {
            writer.name(intervalFacet.getField());
            writer.beginArray();
            for (final IntervalFacet.Count count : intervalFacet.getIntervals()) {
                writer.beginObject();
                writer.name("facet").value(count.getKey());
                writer.name("count").value(count.getCount());
                writer.endObject();
            }
            writer.endArray();
        }
        writer.endObject();
        writer.endObject();
    }
}


