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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat;
import org.apache.nifi.processors.elasticsearch.api.JsonQueryParameters;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public abstract class AbstractJsonQueryElasticsearch<Q extends JsonQueryParameters> extends AbstractProcessor implements ElasticsearchRestProcessor {
    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("All original flowfiles that don't cause an error to occur go to this relationship.").build();
    public static final Relationship REL_HITS = new Relationship.Builder().name("hits")
            .description("Search hits are routed to this relationship.")
            .build();
    public static final Relationship REL_AGGREGATIONS = new Relationship.Builder().name("aggregations")
            .description("Aggregations are routed to this relationship.")
            .build();

    public static final PropertyDescriptor SEARCH_RESULTS_SPLIT = new PropertyDescriptor.Builder()
            .name("el-rest-split-up-hits")
            .displayName("Search Results Split")
            .description("Output a flowfile containing all hits or one flowfile for each individual hit.")
            .allowableValues(ResultOutputStrategy.getNonPaginatedResponseOutputStrategies())
            .defaultValue(ResultOutputStrategy.PER_RESPONSE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SEARCH_RESULTS_FORMAT = new PropertyDescriptor.Builder()
            .name("el-rest-format-hits")
            .displayName("Search Results Format")
            .description("Format of Hits output.")
            .allowableValues(SearchResultsFormat.class)
            .defaultValue(SearchResultsFormat.FULL)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor AGGREGATION_RESULTS_SPLIT = new PropertyDescriptor.Builder()
            .name("el-rest-split-up-aggregations")
            .displayName("Aggregation Results Split")
            .description("Output a flowfile containing all aggregations or one flowfile for each individual aggregation.")
            .allowableValues(ResultOutputStrategy.getNonPaginatedResponseOutputStrategies())
            .defaultValue(ResultOutputStrategy.PER_RESPONSE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor AGGREGATION_RESULTS_FORMAT = new PropertyDescriptor.Builder()
            .name("el-rest-format-aggregations")
            .displayName("Aggregation Results Format")
            .description("Format of Aggregation output.")
            .allowableValues(AggregationResultsFormat.class)
            .defaultValue(AggregationResultsFormat.FULL)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor OUTPUT_NO_HITS = new PropertyDescriptor.Builder()
            .name("el-rest-output-no-hits")
            .displayName("Output No Hits")
            .description("Output a \"" + REL_HITS.getName() + "\" flowfile even if no hits found for query. " +
                    "If true, an empty \"" + REL_HITS.getName() + "\" flowfile will be output even if \"" +
                    REL_AGGREGATIONS.getName() + "\" are output.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final Set<Relationship> relationships = Set.of(REL_ORIGINAL, REL_FAILURE, REL_HITS, REL_AGGREGATIONS);
    static final List<PropertyDescriptor> queryPropertyDescriptors = List.of(
            QUERY_DEFINITION_STYLE,
            QUERY,
            QUERY_CLAUSE,
            SIZE,
            SORT,
            AGGREGATIONS,
            FIELDS,
            SCRIPT_FIELDS,
            QUERY_ATTRIBUTE,
            INDEX,
            TYPE,
            MAX_JSON_FIELD_STRING_LENGTH,
            CLIENT_SERVICE,
            SEARCH_RESULTS_SPLIT,
            SEARCH_RESULTS_FORMAT,
            AGGREGATION_RESULTS_SPLIT,
            AGGREGATION_RESULTS_FORMAT,
            OUTPUT_NO_HITS
    );

    ResultOutputStrategy hitStrategy;
    private SearchResultsFormat hitFormat;
    private ResultOutputStrategy aggregationStrategy;
    private AggregationResultsFormat aggregationFormat;
    private boolean outputNoHits;

    ObjectMapper mapper;

    final AtomicReference<ElasticSearchClientService> clientService = new AtomicReference<>(null);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return queryPropertyDescriptors;
    }

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
    public boolean isIndexNotExistSuccessful() {
        return false;
    }

    boolean isOutputNoHits() {
        return outputNoHits;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService.set(context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class));

        hitStrategy = context.getProperty(SEARCH_RESULTS_SPLIT).asAllowableValue(ResultOutputStrategy.class);
        hitFormat = context.getProperty(SEARCH_RESULTS_FORMAT).asAllowableValue(SearchResultsFormat.class);
        aggregationStrategy = context.getProperty(AGGREGATION_RESULTS_SPLIT).isSet() ? context.getProperty(AGGREGATION_RESULTS_SPLIT).asAllowableValue(ResultOutputStrategy.class) : null;
        aggregationFormat = context.getProperty(AGGREGATION_RESULTS_FORMAT).isSet() ? context.getProperty(AGGREGATION_RESULTS_FORMAT).asAllowableValue(AggregationResultsFormat.class) : null;

        outputNoHits = context.getProperty(OUTPUT_NO_HITS).asBoolean();

        mapper = buildObjectMapper(context);
    }

    @OnStopped
    public void onStopped() {
        clientService.set(null);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();

            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        try {
            final Q queryJsonParameters = buildJsonQueryParameters(input, context, session);

            List<FlowFile> hitsFlowFiles = new ArrayList<>();
            final StopWatch stopWatch = new StopWatch(true);
            final SearchResponse response = doQuery(queryJsonParameters, hitsFlowFiles, session, context, input, stopWatch);

            finishQuery(input, queryJsonParameters, session, context, response);
        } catch (final ElasticsearchException ese) {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Routing to retry." : "Routing to failure");
            getLogger().error(msg, ese);
            if (input != null) {
                session.penalize(input);
                input = session.putAttribute(input, "elasticsearch.query.error", ese.getMessage());
                session.transfer(input, ese.isElastic() ? REL_RETRY : REL_FAILURE);
            }
        } catch (final Exception ex) {
            getLogger().error("Could not query documents.", ex);
            if (input != null) {
                input = session.putAttribute(input, "elasticsearch.query.error", ex.getMessage());
                session.transfer(input, REL_FAILURE);
            }
            context.yield();
        }
    }

    abstract Q buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session) throws IOException;

    void populateCommonJsonQueryParameters(final Q queryJsonParameters, final FlowFile input, final ProcessContext context,
                                           final ProcessSession session) throws IOException {
        final String query = getQuery(input, context, session, mapper);
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
        final String type = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();
        final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).isSet()
                ? context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue()
                : null;

        queryJsonParameters.setQuery(query);
        queryJsonParameters.setIndex(index);
        queryJsonParameters.setType(type);
        queryJsonParameters.setQueryAttr(queryAttr);
    }

    abstract SearchResponse doQuery(final Q queryJsonParameters, final List<FlowFile> hitsFlowFiles, final ProcessSession session,
                                    final ProcessContext context, final FlowFile input, final StopWatch stopWatch) throws IOException;

    abstract void finishQuery(final FlowFile input, final Q queryParameters, final ProcessSession session, final ProcessContext context,
                              final SearchResponse response) throws IOException;

    FlowFile createChildFlowFile(final ProcessSession session, final FlowFile parent) {
        return parent != null ? session.create(parent) : session.create();
    }

    private FlowFile writeAggregationFlowFileContents(final String name, final Integer number, final String json,
                                                      final ProcessSession session, final FlowFile aggFlowFile,
                                                      final Map<String, String> attributes) {
        FlowFile ff = session.write(aggFlowFile, out -> out.write(json.getBytes()));
        Map<String, String> latestAttributes = new HashMap<>(attributes);
        if (name != null) {
            latestAttributes.put("aggregation.name", name);
        }
        if (number != null) {
            latestAttributes.put("aggregation.number", number.toString());
        }

        return session.putAllAttributes(ff, latestAttributes);
    }

    private void handleAggregations(final Map<String, Object> aggregations, final ProcessSession session,
                                    final FlowFile parent, final Map<String, String> attributes,
                                    final String transitUri, final StopWatch stopWatch) throws IOException {
        if (aggregations != null && !aggregations.isEmpty()) {
            final Map<String, Object> formattedAggregations = formatAggregations(aggregations);
            final List<FlowFile> aggsFlowFiles = new ArrayList<>();

            if (aggregationStrategy == ResultOutputStrategy.PER_HIT) {
                int aggCount = 0;
                for (final Map.Entry<String, Object> agg : formattedAggregations.entrySet()) {
                    final FlowFile aggFlowFile = createChildFlowFile(session, parent);
                    final String aggJson = mapper.writeValueAsString(agg.getValue());
                    aggsFlowFiles.add(writeAggregationFlowFileContents(agg.getKey(), ++aggCount, aggJson, session, aggFlowFile, attributes));
                }
            } else {
                final FlowFile aggFlowFile = createChildFlowFile(session, parent);
                final String json = mapper.writeValueAsString(formattedAggregations);
                aggsFlowFiles.add(writeAggregationFlowFileContents(null, null, json, session, aggFlowFile, attributes));
            }

            if (!aggsFlowFiles.isEmpty()) {
                session.transfer(aggsFlowFiles, REL_AGGREGATIONS);
                aggsFlowFiles.forEach(ff -> session.getProvenanceReporter().receive(ff, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> formatAggregations(final Map<String, Object> aggregations) {
        final Map<String, Object> formattedAggregations;

        switch (aggregationFormat) {
            case AggregationResultsFormat.METADATA_ONLY -> {
                formattedAggregations = new LinkedHashMap<>(aggregations);
                formattedAggregations.forEach((k, v) -> ((Map<String, Object>) v).remove("buckets"));
            }
            case AggregationResultsFormat.BUCKETS_ONLY ->
                formattedAggregations = aggregations.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> ((Map<String, Object>) e.getValue()).get("buckets"),
                        (k1, k2) -> k1,
                        LinkedHashMap::new
                ));
            default ->
                formattedAggregations = aggregations;
        }

        return formattedAggregations;
    }

    FlowFile writeHitFlowFile(final int count, final String json, final ProcessSession session,
                              final FlowFile hitFlowFile, final Map<String, String> attributes) {
        final FlowFile ff = session.write(hitFlowFile, out -> out.write(json.getBytes()));
        attributes.put("hit.count", Integer.toString(count));

        return session.putAllAttributes(ff, attributes);
    }

    /*
     * The List<FlowFile> hitsFlowFiles parameter and return value are used in order to allow pagination of query results
     * in AbstractPaginatedJsonQueryElasticsearch. The List is created in onTrigger and passed to doQuery => handleResponse => handleHits,
     * for non-paginated queries the return value will always be an empty List as the FlowFiles will have been transferred;
     * for paginated queries, the List could contain one (or more) FlowFiles, to which further hits may be appended when the next
     * SearchResponse is processed, i.e. this approach allows recursion for paginated queries, but is unnecessary for single-response queries.
     */
    @SuppressWarnings("unused")
    List<FlowFile> handleHits(final List<Map<String, Object>> hits, final boolean newQuery, final Q queryJsonParameters, final ProcessSession session,
                              final FlowFile parent, final Map<String, String> attributes, final List<FlowFile> hitsFlowFiles,
                              final String transitUri, final StopWatch stopWatch) throws IOException {
        if (hits != null && !hits.isEmpty()) {
            final List<Map<String, Object>> formattedHits = formatHits(hits);

            if (hitStrategy == ResultOutputStrategy.PER_HIT) {
                for (final Map<String, Object> hit : formattedHits) {
                    final FlowFile hitFlowFile = createChildFlowFile(session, parent);
                    final String json = mapper.writeValueAsString(hit);
                    hitsFlowFiles.add(writeHitFlowFile(1, json, session, hitFlowFile, attributes));
                }
            } else {
                final FlowFile hitFlowFile = createChildFlowFile(session, parent);
                final String json = mapper.writeValueAsString(formattedHits);
                hitsFlowFiles.add(writeHitFlowFile(formattedHits.size(), json, session, hitFlowFile, attributes));
            }
        } else if (newQuery && outputNoHits) {
            final FlowFile hitFlowFile = createChildFlowFile(session, parent);
            hitsFlowFiles.add(writeHitFlowFile(0, "", session, hitFlowFile, attributes));
        }

        transferResultFlowFiles(session, hitsFlowFiles, transitUri, stopWatch);

        return hitsFlowFiles;
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> formatHits(final List<Map<String, Object>> hits) {
        final List<Map<String, Object>> formattedHits;

        switch (hitFormat) {
            case SearchResultsFormat.METADATA_ONLY -> {
                formattedHits = hits.stream().map(HashMap::new).collect(Collectors.toList());
                formattedHits.forEach(h -> h.remove("_source"));
            }
            case SearchResultsFormat.SOURCE_ONLY ->
                formattedHits = hits.stream()
                        .map(h -> (Map<String, Object>) h.getOrDefault("_source", Collections.emptyMap()))
                        .toList();
            default ->
                formattedHits = hits;
        }

        return formattedHits;
    }

    private void transferResultFlowFiles(final ProcessSession session, final List<FlowFile> hitsFlowFiles, final String transitUri,
                                         final StopWatch stopWatch) {
        // output any results
        if (!hitsFlowFiles.isEmpty()) {
            session.transfer(hitsFlowFiles, REL_HITS);
            hitsFlowFiles.forEach(ff -> session.getProvenanceReporter().receive(ff, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS)));
            hitsFlowFiles.clear();
        }
    }

    List<FlowFile> handleResponse(final SearchResponse response, final boolean newQuery, final Q queryJsonParameters,
                                  final List<FlowFile> hitsFlowFiles, final ProcessSession session, final FlowFile input,
                                  final StopWatch stopWatch) throws IOException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        if (StringUtils.isNotBlank(queryJsonParameters.getQueryAttr())) {
            attributes.put(queryJsonParameters.getQueryAttr(), queryJsonParameters.getQuery());
        }

        final String transitUri = clientService.get().getTransitUrl(queryJsonParameters.getIndex(), queryJsonParameters.getType());
        if (newQuery) {
            // only output aggregations from initial query
            // (omitted from subsequent pages as aggs are calculated across the entire result set, not per page)
            handleAggregations(response.getAggregations(), session, input, attributes, transitUri, stopWatch);
        }

        final List<FlowFile> resultFlowFiles = handleHits(response.getHits(), newQuery, queryJsonParameters, session, input,
                attributes, hitsFlowFiles, transitUri, stopWatch);
        queryJsonParameters.addHitCount(response.getHits().size());

        return resultFlowFiles;
    }
}
