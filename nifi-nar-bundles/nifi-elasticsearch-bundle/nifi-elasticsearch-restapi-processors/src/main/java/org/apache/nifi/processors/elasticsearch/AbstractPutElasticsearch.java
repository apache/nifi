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
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public abstract class AbstractPutElasticsearch extends AbstractProcessor implements ElasticsearchRestProcessor {
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-es-record-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of FlowFiles to send over in a single batch.")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
        .name("put-es-record-index-op")
        .displayName("Index Operation")
        .description("The type of the operation used to index (create, delete, index, update, upsert)")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(IndexOperationRequest.Operation.Index.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor OUTPUT_ERROR_RESPONSES = new PropertyDescriptor.Builder()
            .name("put-es-output-error-responses")
            .displayName("Output Error Responses")
            .description("If this is enabled, response messages from Elasticsearch marked as \"error\" will be output to the \"error_responses\" relationship." +
                    "This does not impact the output of flowfiles to the \"success\" or \"errors\" relationships")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All flowfiles that succeed in being transferred into Elasticsearch go here. " +
                    "Documents received by the Elasticsearch _bulk API may still result in errors on the Elasticsearch side. " +
                    "The Elasticsearch response will need to be examined to determine whether any Document(s)/Record(s) resulted in errors.")
            .build();

    static final Relationship REL_ERROR_RESPONSES = new Relationship.Builder()
            .name("error_responses")
            .description("Elasticsearch _bulk API responses marked as \"error\" go here " +
                    "(and optionally \"not_found\" when \"Treat \"Not Found\" as Error\" is \"true\").")
            .build();

    static final List<String> ALLOWED_INDEX_OPERATIONS = Collections.unmodifiableList(Arrays.asList(
            IndexOperationRequest.Operation.Create.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Delete.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Index.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Update.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Upsert.getValue().toLowerCase()
    ));

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>(getBaseRelationships());

    boolean logErrors;
    boolean outputErrorResponses;
    boolean notFoundIsSuccessful;
    ObjectMapper errorMapper;

    final AtomicReference<ElasticSearchClientService> clientService = new AtomicReference<>(null);

    abstract Set<Relationship> getBaseRelationships();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
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
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (OUTPUT_ERROR_RESPONSES.equals(descriptor)) {
            final Set<Relationship> newRelationships = new HashSet<>(getBaseRelationships());
            if (Boolean.parseBoolean(newValue)) {
                newRelationships.add(REL_ERROR_RESPONSES);
            }
            relationships.set(newRelationships);
        }
    }

    @Override
    public boolean isIndexNotExistSuccessful() {
        // index can be created during _bulk index/create operation
        return true;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService.set(context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class));

        this.logErrors = context.getProperty(LOG_ERROR_RESPONSES).asBoolean();
        this.outputErrorResponses = context.getProperty(OUTPUT_ERROR_RESPONSES).asBoolean();

        if (errorMapper == null && (outputErrorResponses || logErrors || getLogger().isDebugEnabled())) {
            errorMapper = new ObjectMapper();
            errorMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
    }

    @OnStopped
    public void onStopped() {
        clientService.set(null);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();

        final PropertyValue indexOp = validationContext.getProperty(INDEX_OP);
        final ValidationResult.Builder indexOpValidationResult = new ValidationResult.Builder().subject(INDEX_OP.getName());
        if (!indexOp.isExpressionLanguagePresent()) {
            final String indexOpValue = indexOp.evaluateAttributeExpressions().getValue();
            indexOpValidationResult.input(indexOpValue);
            if (!ALLOWED_INDEX_OPERATIONS.contains(indexOpValue.toLowerCase())) {
                indexOpValidationResult.valid(false)
                        .explanation(String.format("%s must be Expression Language or one of %s",
                                INDEX_OP.getDisplayName(), ALLOWED_INDEX_OPERATIONS)
                        );
            } else {
                indexOpValidationResult.valid(true);
            }
        } else {
            indexOpValidationResult.valid(true).input(indexOp.getValue()).explanation("Expression Language present");
        }
        validationResults.add(indexOpValidationResult.build());

        return validationResults;
    }

    void transferFlowFilesOnException(final Exception ex, final Relationship rel, final ProcessSession session,
                                      final boolean penalize, final FlowFile... flowFiles) {
        for (FlowFile flowFile : flowFiles) {
            flowFile = session.putAttribute(flowFile, "elasticsearch.put.error", ex.getMessage() == null ? "null" : ex.getMessage());
            if (penalize) {
                session.penalize(flowFile);
            }
            session.transfer(flowFile, rel);
        }
    }

    void handleElasticsearchDocumentErrors(final Map<Integer, Map<String, Object>> errors, final ProcessSession session, final FlowFile parent) throws IOException {
        if (!errors.isEmpty() && (outputErrorResponses || logErrors || getLogger().isDebugEnabled())) {
            if (logErrors || getLogger().isDebugEnabled()) {
                final String output = String.format(
                        "An error was encountered while processing bulk operations. Server response below:%n%n%s",
                        errorMapper.writeValueAsString(errors.values())
                );

                if (logErrors) {
                    getLogger().error(output);
                } else {
                    getLogger().debug(output);
                }
            }

            if (outputErrorResponses) {
                FlowFile errorResponsesFF = null;
                try {
                    errorResponsesFF = session.create(parent);
                    try (final OutputStream errorsOutputStream = session.write(errorResponsesFF)) {
                        errorMapper.writeValue(errorsOutputStream, errors.values());
                    }
                    errorResponsesFF = session.putAttribute(errorResponsesFF, "elasticsearch.put.error.count", String.valueOf(errors.size()));
                    session.transfer(errorResponsesFF, REL_ERROR_RESPONSES);
                } catch (final IOException ex) {
                    getLogger().error("Unable to write error responses", ex);
                    session.remove(errorResponsesFF);
                    throw ex;
                }
            }
        }
    }

    Predicate<Map<String, Object>> isElasticsearchError() {
        return inner -> inner.containsKey("error");
    }

    Predicate<Map<String, Object>> isElasticsearchNotFound() {
        return inner -> inner.containsKey("result") && "not_found".equals(inner.get("result"));
    }

    final Map<Integer, Map<String, Object>> findElasticsearchResponseErrors(final IndexOperationResponse response) {
        final Map<Integer, Map<String, Object>> errors = new LinkedHashMap<>(response.getItems() == null ? 0 : response.getItems().size(), 1);

        final List<Predicate<Map<String, Object>>> errorItemFilters = new ArrayList<>(2);
        if (response.hasErrors()) {
            errorItemFilters.add(isElasticsearchError());
        }
        if (!notFoundIsSuccessful) {
            errorItemFilters.add(isElasticsearchNotFound());
        }

        if (response.getItems() != null && !errorItemFilters.isEmpty()) {
            for (int index = 0; index < response.getItems().size(); index++) {
                final Map<String, Object> current = response.getItems().get(index);
                if (!current.isEmpty()) {
                    final String key = current.keySet().stream().findFirst().orElse(null);
                    @SuppressWarnings("unchecked") final Map<String, Object> inner = (Map<String, Object>) current.get(key);
                    if (inner != null && errorItemFilters.stream().anyMatch(p -> p.test(inner))) {
                        errors.put(index, inner);
                    }
                }
            }
        }
        return errors;
    }
}
