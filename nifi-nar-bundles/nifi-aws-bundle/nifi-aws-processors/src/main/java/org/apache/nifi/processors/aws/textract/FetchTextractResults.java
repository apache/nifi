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

package org.apache.nifi.processors.aws.textract;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractAsync;
import com.amazonaws.services.textract.AmazonTextractAsyncClient;
import com.amazonaws.services.textract.AmazonTextractAsyncClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DocumentMetadata;
import com.amazonaws.services.textract.model.ExpenseDocument;
import com.amazonaws.services.textract.model.ExpenseField;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentAnalysisResult;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.GetExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.GetExpenseAnalysisResult;
import com.amazonaws.services.textract.model.InternalServerErrorException;
import com.amazonaws.services.textract.model.JobStatus;
import com.amazonaws.services.textract.model.LineItemFields;
import com.amazonaws.services.textract.model.LineItemGroup;
import com.amazonaws.services.textract.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.textract.model.ThrottlingException;
import com.amazonaws.services.textract.model.Warning;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.stream.io.NonCloseableOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

@CapabilityDescription("Retrieves the results of a Textract job that was triggered via StartTextractJob. If the job has not completed yet, the incoming FlowFile will be routed to the 'in progress' " +
    "Relationship, providing the ability to retry after the Penalization period. Otherwise, the original FlowFile will be transferred to the 'original' relationship while the results of the " +
    "Textract job are transferred as JSON to the 'success' or 'partial success' relationship. This processor is typically used in conjunction with GetSQS and/or StartTextractJob. See Additional " +
    "Details for more information.")
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"aws", "cloud", "text", "textract", "unstructured", "detect", "analyze"})
@SeeAlso({AWSCredentialsProviderService.class, StartTextractJob.class, InvokeTextract.class})
public class FetchTextractResults extends AbstractAWSCredentialsProviderProcessor<AmazonTextractAsyncClient> {
    private static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";
    private static String JSON_MIME_TYPE = "application/json";

    static final PropertyDescriptor ACTION = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(TextractProperties.ACTION)
        .defaultValue(TextractProperties.USE_ATTRIBUTE.getValue())
        .build();

    static final PropertyDescriptor JOB_ID = new PropertyDescriptor.Builder()
        .name("Textract Job ID")
        .displayName("Textract Job ID")
        .description("The ID of the Textract Job whose results should be fetched")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${textract.job.id}")
        .build();

    static final PropertyDescriptor TEXT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("Text Attribute Name")
        .displayName("Text Attribute Name")
        .description("Specifies the name of an attribute to add to the outbound FlowFiles that include the text that was detected. If not specified, the text will not be added as an attribute.")
        .required(false)
        .dependsOn(TextractProperties.ACTION, TextractProperties.DETECT_TEXT, TextractProperties.ANALYZE_DOCUMENT, TextractProperties.USE_ATTRIBUTE)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .build();
    static final PropertyDescriptor TEXT_CONFIDENCE_THRESHOLD = new PropertyDescriptor.Builder()
        .name("Text Confidence Threshold")
        .displayName("Text Confidence Threshold")
        .description("Specifies the minimum confidence that Textract must have in order for a line of text to be included in the attribute specified by the <Text Attribute Name> property")
        .required(false)
        .addValidator(StandardValidators.createLongValidator(1, 100, true))
        .dependsOn(TEXT_ATTRIBUTE_NAME)
        .defaultValue("70")
        .build();
    static final PropertyDescriptor MAX_CHARACTER_COUNT = new PropertyDescriptor.Builder()
        .name("Max Character Count")
        .displayName("Max Character Count")
        .description("If the number of characters in the text exceed this threshold, the text will be truncated so as not to create a very large attribute.")
        .required(false)
        .addValidator(StandardValidators.createLongValidator(1, 16384, true))
        .defaultValue("4096")
        .dependsOn(TEXT_ATTRIBUTE_NAME)
        .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        ACTION,
        JOB_ID,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        TEXT_ATTRIBUTE_NAME,
        TEXT_CONFIDENCE_THRESHOLD,
        MAX_CHARACTER_COUNT,
        TextractProperties.LINE_ITEM_ATTRIBUTE_PREFIX,
        TextractProperties.SUMMARY_ITEM_ATTRIBUTE_PREFIX,
        REGION,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD));

    // Relationships
    private static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Upon successful completion, the original FlowFile will be routed to this relationship while the results of the Textract job will be routed to the 'success' relationship")
        .autoTerminateDefault(true)
        .build();
    private static final Relationship REL_PARTIAL_SUCCESS = new Relationship.Builder()
        .name("partial success")
        .description("The Textract job has completed but was only partially successful")
        .build();
    private static final Relationship REL_IN_PROGRESS = new Relationship.Builder()
        .name("in progress")
        .description("The Textract job is currently still being processed")
        .build();
    static final Relationship REL_TRANSIENT_FAILURE = new Relationship.Builder()
        .name("transient failure")
        .description("Retrieving the AWS Textract results failed for some reason, but the issue is likely to resolve on its own, such as Provisioned Throughput Exceeded or a Throttling failure. " +
            "It is generally expected to retry this relationship.")
        .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_ORIGINAL,
        REL_SUCCESS,
        REL_PARTIAL_SUCCESS,
        REL_IN_PROGRESS,
        REL_TRANSIENT_FAILURE,
        REL_FAILURE
    )));

    private final AtomicReference<ObjectMapper> objectMapperReference = new AtomicReference<>();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected AmazonTextractAsyncClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration clientConfig) {
        final AmazonTextractAsyncClientBuilder clientBuilder = AmazonTextractAsyncClientBuilder.standard();
        clientBuilder.setRegion(context.getProperty(REGION).getValue());
        clientBuilder.setCredentials(credentialsProvider);
        clientBuilder.setClientConfiguration(clientConfig);
        final AmazonTextractAsync client = clientBuilder.build();
        return (AmazonTextractAsyncClient) client;
    }

    @Override
    protected boolean isInitializeRegionAndEndpoint() {
        return false;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String actionName = TextractProperties.getAction(context, flowFile);
        final String jobId = context.getProperty(JOB_ID).evaluateAttributeExpressions(flowFile).getValue();

        final JobResultsFetcher jobResults = getJobResults(actionName);
        if (jobResults == null) {
            getLogger().error("Cannot analyze text from {} because the Textract Action provided is invalid: {}", flowFile, actionName);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            jobResults.processFlowFile(context, session, flowFile, jobId);

            final String transitUri = "https://textract." + context.getProperty(REGION).getValue() + ".amazonaws.com/jobs/" + jobId;
            session.getProvenanceReporter().invokeRemoteProcess(jobResults.getCreatedFlowFile(), transitUri);
        } catch (final InternalServerErrorException | ThrottlingException | ProvisionedThroughputExceededException e) {
            getLogger().error("Failed to retrieve results for {}. Routing to {}", flowFile, REL_TRANSIENT_FAILURE.getName(), e);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_TRANSIENT_FAILURE);

            final FlowFile created = jobResults.getCreatedFlowFile();
            if (created != null) {
                session.remove(created);
            }

            context.yield();
        } catch (final Throwable t) {
            getLogger().error("Failed to retrieve results for {}. Routing to {}", flowFile, REL_FAILURE.getName(), t);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, t.getMessage());
            session.transfer(flowFile, REL_FAILURE);

            final FlowFile created = jobResults.getCreatedFlowFile();
            if (created != null) {
                session.remove(created);
            }
        }
    }

    @Override
    protected AmazonTextractAsyncClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return null;
    }

    private JobResultsFetcher getJobResults(final String actionName) {
        if (actionName.equalsIgnoreCase(TextractProperties.DETECT_TEXT.getValue())) {
            return new DetectDocumentResultsFetcher();
        }
        if (actionName.equalsIgnoreCase(TextractProperties.ANALYZE_DOCUMENT.getValue())) {
            return new AnalyzeDocumentResultsFetcher();
        }
        if (actionName.equalsIgnoreCase(TextractProperties.ANALYZE_EXPENSE.getValue())) {
            return new AnalyzeExpenseResultsFetcher();
        }

        return null;
    }

    private interface JobResultsFetcher {
        /**
         * Process the given FlowFile, creating a new child FlowFile that contains the results of the Textract job.
         * If the textract job is still being processed, the given FlowFile should be transferred to {@link #REL_IN_PROGRESS}.
         * If the textract job has failed, the given FlowFile should be transferred to {@link #REL_FAILURE}.
         * If the textract job has partially succeeded, the given FlowFile should be transferred to {@link #REL_ORIGINAL} and the result transferred to {@link #REL_PARTIAL_SUCCESS}.
         * If the textract job has succeeded, the given FlowFile should be transferred to {@link #REL_ORIGINAL} and the result transferred to {@link #REL_SUCCESS}.
         * If any Throwable is thrown, the given FlowFile should not be transferred anywhere, and any FlowFile that was created must be made
         * available via the {@link #getCreatedFlowFile()} method.
         */
        void processFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, String jobId) throws Throwable;

        FlowFile getCreatedFlowFile();
    }

    private ObjectMapper getObjectMapper() {
        // Lazily initialize the ObjectMapper.
        ObjectMapper mapper = objectMapperReference.get();
        if (mapper != null) {
            return mapper;
        }

        mapper = new ObjectMapper();
        objectMapperReference.compareAndSet(null, mapper);
        return mapper;
    }

    private void writeAsJson(final Collection<?> values, final OutputStream out) throws IOException {
        final ObjectMapper mapper = getObjectMapper();
        final JsonFactory jsonFactory = new JsonFactory(mapper);

        try (final OutputStream blockingOut = new NonCloseableOutputStream(out);
            final JsonGenerator jsonGenerator = jsonFactory.createGenerator(blockingOut)) {
            jsonGenerator.writeStartArray();

            for (final Object value : values) {
                jsonGenerator.writeObject(value);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.flush();
        }
    }

    /**
     * Processing of the Text Detection and Text Analysis are basically identical, but the AWS API provides a different class for each,
     * with methods that have the same signature. Because of this, we cannot process them both using the same methods. As a result, we have
     * an abstract implementation that is responsible for handling all processing with getter methods that transform the Response object into the
     * objects that we want to retrieve from the response.
     */
    private abstract class DetectionAnalysisResultsFetcher<T> implements JobResultsFetcher {
        private FlowFile createdFlowFile = null;

        @Override
        public FlowFile getCreatedFlowFile() {
            return createdFlowFile;
        }

        public abstract Future<T> makeRequest(ProcessContext context, FlowFile flowFile, String jobId, String nextToken);

        public abstract String getJobStatus(T response);
        public abstract String getStatusMessage(T response);
        public abstract List<Block> getBlocks(T response);
        public abstract List<Warning> getWarnings(T response);
        public abstract DocumentMetadata getDocumentMetadata(T response);
        public abstract String getModelVersion(T response);
        public abstract String getNextToken(T response);

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final String jobId) throws Throwable {
            T result;
            try {
                result = makeRequest(context, flowFile, jobId, null).get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }

            final JobStatus jobStatus = JobStatus.fromValue(getJobStatus(result));

            if (jobStatus == JobStatus.IN_PROGRESS) {
                session.penalize(flowFile);
                session.transfer(flowFile, REL_IN_PROGRESS);
                return;
            }

            if (jobStatus == JobStatus.FAILED) {
                final String failureReason = getStatusMessage(result);
                session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Textract reported that the task failed for {}: {}", flowFile, failureReason);
                return;
            }

            final Relationship relationship;
            if (jobStatus == JobStatus.PARTIAL_SUCCESS) {
                relationship = REL_PARTIAL_SUCCESS;
            } else {
                relationship = REL_SUCCESS;
            }

            createdFlowFile = session.create(flowFile);

            int blockCount = getBlocks(result).size();
            final List<Warning> warnings = new ArrayList<>(getWarnings(result));
            final Map<String, String> attributes = new HashMap<>();

            final DocumentMetadata metadata = getDocumentMetadata(result);
            final Integer pages = metadata.getPages();
            if (pages != null) {
                attributes.put("num.pages", pages.toString());
            }

            attributes.put("aws.model.version", getModelVersion(result));

            final String textAttributeName = context.getProperty(TEXT_ATTRIBUTE_NAME).getValue();
            final Integer confidenceThreshold = context.getProperty(TEXT_CONFIDENCE_THRESHOLD).asInteger();
            final Integer maxChars = context.getProperty(MAX_CHARACTER_COUNT).asInteger();

            try (final OutputStream out = session.write(createdFlowFile)) {
                List<Block> blocks = getBlocks(result);
                writeAsJson(blocks, out);

                final StringBuilder textBuilder = new StringBuilder();
                if (textAttributeName != null) {
                    final String text = TextractProperties.lineBlocksToString(blocks, confidenceThreshold, attributes);
                    textBuilder.append(text);
                    if (textBuilder.length() > maxChars) {
                        textBuilder.setLength(maxChars);
                    }
                }

                while (getNextToken(result) != null) {
                    final String nextToken = getNextToken(result);

                    final GetDocumentTextDetectionRequest nextRequest = new GetDocumentTextDetectionRequest();
                    nextRequest.setJobId(jobId);
                    nextRequest.setNextToken(nextToken);

                    try {
                        result = makeRequest(context, flowFile, jobId, nextToken).get();
                    } catch (final ExecutionException ee) {
                        throw ee.getCause();
                    }

                    blocks = getBlocks(result);
                    blockCount += blocks.size();
                    warnings.addAll(getWarnings(result));
                    writeAsJson(blocks, out);

                    if (textAttributeName != null) {
                        final String text = TextractProperties.lineBlocksToString(blocks, confidenceThreshold, attributes);
                        textBuilder.append(text);
                        if (textBuilder.length() > maxChars) {
                            textBuilder.setLength(maxChars);
                        }
                    }
                }

                if (textAttributeName != null) {
                    attributes.put(textAttributeName, textBuilder.toString());
                }
            }

            int warningIndex=0;
            for (final Warning warning : warnings) {
                final String attributePrefix = "textract.warnings." + warningIndex + ".";
                attributes.put(attributePrefix + "errorCode", warning.getErrorCode());
                attributes.put(attributePrefix + "affected.pages", pagesToString(warning.getPages()));
                warningIndex++;
            }

            attributes.put("num.text.blocks", String.valueOf(blockCount));

            // On Success, transfer original & created FlowFiles
            session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_ORIGINAL);

            attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
            session.putAllAttributes(createdFlowFile, attributes);
            session.transfer(createdFlowFile, relationship);
        }

        private String pagesToString(final List<Integer> pages) {
            if (pages.isEmpty()) {
                return "";
            }
            if (pages.size() == 1) {
                return String.valueOf(pages.get(0));
            }

            final StringBuilder sb = new StringBuilder();
            int i=0;
            for (final Integer page : pages) {
                if (i++ > 0) {
                    sb.append(", ");
                }

                sb.append(page);
            }

            return sb.toString();
        }
    }

    private class DetectDocumentResultsFetcher extends DetectionAnalysisResultsFetcher<GetDocumentTextDetectionResult> {

        @Override
        public Future<GetDocumentTextDetectionResult> makeRequest(final ProcessContext context, final FlowFile flowFile, final String jobId, final String nextToken) {
            final GetDocumentTextDetectionRequest request = new GetDocumentTextDetectionRequest();
            request.setJobId(jobId);
            request.setNextToken(nextToken);

            return client.getDocumentTextDetectionAsync(request);
        }

        @Override
        public String getJobStatus(final GetDocumentTextDetectionResult result) {
            return result.getJobStatus();
        }

        @Override
        public String getStatusMessage(final GetDocumentTextDetectionResult result) {
            return result.getStatusMessage();
        }

        @Override
        public List<Block> getBlocks(final GetDocumentTextDetectionResult result) {
            return result.getBlocks();
        }

        @Override
        public List<Warning> getWarnings(final GetDocumentTextDetectionResult result) {
            final List<Warning> warnings = result.getWarnings();
            return warnings == null ? Collections.emptyList() : warnings;
        }

        @Override
        public DocumentMetadata getDocumentMetadata(final GetDocumentTextDetectionResult result) {
            return result.getDocumentMetadata();
        }

        @Override
        public String getModelVersion(final GetDocumentTextDetectionResult result) {
            return result.getDetectDocumentTextModelVersion();
        }

        @Override
        public String getNextToken(final GetDocumentTextDetectionResult result) {
            return result.getNextToken();
        }
    }

    private class AnalyzeDocumentResultsFetcher extends DetectionAnalysisResultsFetcher<GetDocumentAnalysisResult> {

        @Override
        public Future<GetDocumentAnalysisResult> makeRequest(final ProcessContext context, final FlowFile flowFile, final String jobId, final String nextToken) {
            final GetDocumentAnalysisRequest request = new GetDocumentAnalysisRequest();
            request.setJobId(jobId);
            request.setNextToken(nextToken);

            return client.getDocumentAnalysisAsync(request);
        }

        @Override
        public String getJobStatus(final GetDocumentAnalysisResult result) {
            return result.getJobStatus();
        }

        @Override
        public String getStatusMessage(final GetDocumentAnalysisResult result) {
            return result.getStatusMessage();
        }

        @Override
        public List<Block> getBlocks(final GetDocumentAnalysisResult result) {
            return result.getBlocks();
        }

        @Override
        public List<Warning> getWarnings(final GetDocumentAnalysisResult result) {
            final List<Warning> warnings = result.getWarnings();
            return warnings == null ? Collections.emptyList() : warnings;
        }

        @Override
        public DocumentMetadata getDocumentMetadata(final GetDocumentAnalysisResult result) {
            return result.getDocumentMetadata();
        }

        @Override
        public String getModelVersion(final GetDocumentAnalysisResult result) {
            return result.getAnalyzeDocumentModelVersion();
        }

        @Override
        public String getNextToken(final GetDocumentAnalysisResult result) {
            return result.getNextToken();
        }
    }



    private class AnalyzeExpenseResultsFetcher implements JobResultsFetcher {
        private FlowFile createdFlowFile = null;

        private GetExpenseAnalysisResult makeRequest(final String jobId, final String nextToken) throws Throwable {
            final GetExpenseAnalysisRequest request = new GetExpenseAnalysisRequest();
            request.setJobId(jobId);
            request.setNextToken(nextToken);

            try {
                return client.getExpenseAnalysisAsync(request).get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final String jobId) throws Throwable {
            GetExpenseAnalysisResult result = makeRequest(jobId, null);
            final JobStatus jobStatus = JobStatus.fromValue(result.getJobStatus());

            if (jobStatus == JobStatus.IN_PROGRESS) {
                session.penalize(flowFile);
                session.transfer(flowFile, REL_IN_PROGRESS);
                return;
            }

            if (jobStatus == JobStatus.FAILED) {
                final String failureReason = result.getStatusMessage();
                session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final Relationship relationship;
            if (jobStatus == JobStatus.PARTIAL_SUCCESS) {
                relationship = REL_PARTIAL_SUCCESS;
            } else {
                relationship = REL_SUCCESS;
            }


            List<ExpenseDocument> expenseDocuments = result.getExpenseDocuments();
            final DocumentMetadata documentMetadata = result.getDocumentMetadata();

            final Map<String, String> attributes = new HashMap<>();
            final Integer pages = documentMetadata.getPages();
            if (pages != null) {
                attributes.put("num.pages", pages.toString());
            }

            int lineItemNumber = 0;

            final String lineItemPrefix = context.getProperty(TextractProperties.LINE_ITEM_ATTRIBUTE_PREFIX).getValue();
            final String summaryItemPrefix = context.getProperty(TextractProperties.SUMMARY_ITEM_ATTRIBUTE_PREFIX).getValue();

            createdFlowFile = session.create(flowFile);
            try (final OutputStream out = session.write(createdFlowFile)) {
                while (true) {
                    lineItemNumber = handleDocuments(result.getExpenseDocuments(), out, lineItemPrefix, summaryItemPrefix, attributes, lineItemNumber);

                    final String nextToken = result.getNextToken();
                    if (nextToken == null) {
                        break;
                    }

                    result = makeRequest(jobId, nextToken);
                }
            }

            attributes.put("line.item.count", String.valueOf(lineItemNumber));

            // Add attributes to FlowFiles and transfer result to analysis relationship
            session.putAllAttributes(flowFile, attributes);

            attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
            createdFlowFile = session.putAllAttributes(createdFlowFile, attributes);

            session.transfer(createdFlowFile, relationship);
            session.transfer(flowFile, REL_ORIGINAL);
        }

        private int handleDocuments(final List<ExpenseDocument> expenseDocuments, final OutputStream out, final String lineItemPrefix,
                                    final String summaryItemPrefix, final Map<String, String> attributes, final int initialLineItemNumber) throws IOException {
            int lineItemNumber = initialLineItemNumber;

            // Add attributes for line items
            if (lineItemPrefix != null) {
                for (final ExpenseDocument expenseDocument : expenseDocuments) {
                    for (final LineItemGroup group : expenseDocument.getLineItemGroups()) {
                        for (final LineItemFields fields : group.getLineItems()) {
                            for (final ExpenseField expenseField : fields.getLineItemExpenseFields()) {
                                final String prefix = lineItemPrefix + "." + lineItemNumber;
                                final boolean added = addExpenseField(expenseField, attributes, prefix);

                                if (added) {
                                    lineItemNumber++;
                                }
                            }
                        }
                    }
                }
            }

            // Add attributes for summary items
            if (summaryItemPrefix != null) {
                for (final ExpenseDocument expenseDocument : expenseDocuments) {
                    for (final ExpenseField summaryField : expenseDocument.getSummaryFields()) {
                        addExpenseField(summaryField, attributes, summaryItemPrefix);
                    }
                }
            }

            // Write JSON contents to FlowFile
            writeAsJson(expenseDocuments, out);

            return lineItemNumber;
        }

        private boolean addExpenseField(final ExpenseField expenseField, final Map<String, String> attributes, final String attributePrefix) {
            final String expenseType = expenseField.getType().getText();
            final String expenseValue = expenseField.getValueDetection().getText();
            final String label = expenseField.getLabelDetection().getText();

            String attributeSuffix = (label == null || label.trim().isEmpty()) ? expenseType : label;
            if (attributeSuffix == null || attributeSuffix.trim().isEmpty()) {
                return false;
            }

            attributeSuffix = attributeSuffix.replaceAll("\\s+", " ");
            final String attributeName = attributePrefix + "." + attributeSuffix;
            attributes.put(attributeName, expenseValue);

            return true;
        }

        @Override
        public FlowFile getCreatedFlowFile() {
            return createdFlowFile;
        }
    }
}
