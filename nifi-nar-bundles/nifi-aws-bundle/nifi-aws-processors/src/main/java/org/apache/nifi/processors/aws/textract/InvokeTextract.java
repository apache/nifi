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
import com.amazonaws.services.textract.model.AnalyzeDocumentRequest;
import com.amazonaws.services.textract.model.AnalyzeDocumentResult;
import com.amazonaws.services.textract.model.AnalyzeExpenseRequest;
import com.amazonaws.services.textract.model.AnalyzeExpenseResult;
import com.amazonaws.services.textract.model.AnalyzeIDRequest;
import com.amazonaws.services.textract.model.AnalyzeIDResult;
import com.amazonaws.services.textract.model.BadDocumentException;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DetectDocumentTextRequest;
import com.amazonaws.services.textract.model.DetectDocumentTextResult;
import com.amazonaws.services.textract.model.Document;
import com.amazonaws.services.textract.model.DocumentMetadata;
import com.amazonaws.services.textract.model.DocumentTooLargeException;
import com.amazonaws.services.textract.model.ExpenseDocument;
import com.amazonaws.services.textract.model.ExpenseField;
import com.amazonaws.services.textract.model.HumanLoopQuotaExceededException;
import com.amazonaws.services.textract.model.IdentityDocument;
import com.amazonaws.services.textract.model.IdentityDocumentField;
import com.amazonaws.services.textract.model.InternalServerErrorException;
import com.amazonaws.services.textract.model.InvalidParameterException;
import com.amazonaws.services.textract.model.InvalidS3ObjectException;
import com.amazonaws.services.textract.model.LineItemFields;
import com.amazonaws.services.textract.model.LineItemGroup;
import com.amazonaws.services.textract.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.textract.model.ThrottlingException;
import com.amazonaws.services.textract.model.UnsupportedDocumentException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@CapabilityDescription("Sends the contents of the FlowFile to the AWS Textract service in order to perform text-related extraction and analysis on an image. " +
    "Upon successful completion, the original FlowFile will be updated with any relevant attributes, such as the raw extracted text. A second FlowFile will be " +
    "sent to the 'analysis' relationship including the AWS Textract response in JSON form. Note that this Processor will wait until the Textract task completes and then transfer the results along " +
    "as a JSON document. This will typically take several seconds for a single FlowFile. Additionally, the size of the incoming FlowFile is limited to 5-10 MB, depending on the action being " +
    "performed and does not support PDF inputs. If many documents are to be processed or large documents need to be processed, see the StartTextractJob and GetTextractResults processors.")
@SystemResourceConsiderations({
    @SystemResourceConsideration(resource = SystemResource.NETWORK, description = "Contents of the FlowFile are uploaded to AWS"),
    @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Contents of the FlowFile are loaded into memory / Java heap, as is the Textract Response")
})
@SupportsBatching
@Tags({"aws", "cloud", "text", "textract", "unstructured", "detect", "analyze"})
@SeeAlso({AWSCredentialsProviderService.class, StartTextractJob.class})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "A mime.type attribute will be added to the 'analysis' FlowFile with a value of application/json"),
    @WritesAttribute(attribute = "failure.reason", description = "If a FlowFile is routed to failure, a message indicating the nature of the failure will be added")
})
public class InvokeTextract extends AbstractAWSCredentialsProviderProcessor<AmazonTextractAsyncClient> {
    private static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";

    private static String JSON_MIME_TYPE = "application/json";

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
    static final PropertyDescriptor ID_ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
        .name("ID Attribute Name Prefix")
        .displayName("Attribute Name Prefix")
        .description("If specified, all fields that are identified in the document will be added as attributes with this prefix. For example, if this property is set to aws.id then attributes will " +
            "be added such as aws.id.FIRST_NAME, aws.id.LAST_NAME, etc. If no value is specified, attributes will not be added.")
        .required(false)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .dependsOn(TextractProperties.ACTION, TextractProperties.ANALYZE_ID)
        .build();

    private static final List<PropertyDescriptor> propertyDescriptors = Arrays.asList(
        TextractProperties.ACTION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        TEXT_ATTRIBUTE_NAME,
        TEXT_CONFIDENCE_THRESHOLD,
        TextractProperties.ANALYSIS_FEATURES,
        ID_ATTRIBUTE_PREFIX,
        TextractProperties.LINE_ITEM_ATTRIBUTE_PREFIX,
        TextractProperties.SUMMARY_ITEM_ATTRIBUTE_PREFIX,
        REGION,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD
    );


    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Upon successful analysis, the original FlowFile will be routed to the 'original' relationship while the results of the analysis are routed to the 'analysis' relationship")
        .build();
    static final Relationship REL_ANALYSIS = new Relationship.Builder()
        .name("analysis")
        .description("Upon successful analysis, the results of the analysis are routed to the 'analysis' relationship, while the original FlowFile will be routed to the 'original' relationship")
        .build();
    static final Relationship REL_INVALID_INPUT = new Relationship.Builder()
        .name("invalid input")
        .description("AWS Textract indicated that the input was not valid for some reason - for instance, if the input is too large or is not the correct format.")
        .build();
    static final Relationship REL_TRANSIENT_FAILURE = new Relationship.Builder()
        .name("transient failure")
        .description("Triggering the AWS Textract service failed for some reason, but the issue is likely to resolve on its own, such as Provisioned Throughput Exceeded or a Throttling failure. " +
            "It is generally reasonable to retry this relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("This relationship is used for any authentication or authorization failure or if any other unexpected failure is encountered.")
        .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_ORIGINAL,
        REL_ANALYSIS,
        REL_INVALID_INPUT,
        REL_TRANSIENT_FAILURE,
        REL_FAILURE)));

    private final AtomicReference<ObjectMapper> objectMapperReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
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
        final TextractAction action = getAction(actionName);
        if (action == null) {
            getLogger().error("Cannot analyze text from {} because the Textract Action provided is invalid: {}", flowFile, actionName);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            action.processFlowFile(context, session, flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (final BadDocumentException | DocumentTooLargeException | InvalidParameterException | InvalidS3ObjectException | UnsupportedDocumentException e) {
            getLogger().error("Failed to perform configured action for {}. Routing to {}", flowFile, REL_INVALID_INPUT.getName(), e);
            flowFile = session.penalize(flowFile);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_INVALID_INPUT);
        } catch (final HumanLoopQuotaExceededException | InternalServerErrorException | ProvisionedThroughputExceededException | ThrottlingException e) {
            getLogger().error("Failed to perform configured action for {}. Routing to {}", flowFile, REL_TRANSIENT_FAILURE.getName(), e);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_TRANSIENT_FAILURE);
            context.yield();
        } catch (final Exception e) {
            getLogger().error("Failed to process {}", flowFile, e);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private TextractAction getAction(final String action) {
        if (action.equalsIgnoreCase(TextractProperties.DETECT_TEXT.getValue())) {
            return new DetectDocumentTextAction(client);
        }
        if (action.equalsIgnoreCase(TextractProperties.ANALYZE_DOCUMENT.getValue())) {
            return new AnalyzeDocumentAction(client);
        }
        if (action.equalsIgnoreCase(TextractProperties.ANALYZE_EXPENSE.getValue())) {
            return new AnalyzeExpenseAction(client);
        }
        if (action.equalsIgnoreCase(TextractProperties.ANALYZE_ID.getValue())) {
            return new AnalyzeIDAction(client);
        }

        return null;
    }

    @Override
    protected AmazonTextractAsyncClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return null;
    }

    private Document createDocument(final ProcessSession session, final FlowFile flowFile, final int maxFileSize) throws IOException {
        if (flowFile.getSize() > maxFileSize) {
            throw new DocumentTooLargeException("FlowFile is too large: max size of " + maxFileSize + " bytes");
        }

        final byte[] bytes;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            session.exportTo(flowFile, baos);
            bytes = baos.toByteArray();
        }

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        final Document document = new Document();
        document.setBytes(byteBuffer);

        return document;
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

        try (final JsonGenerator jsonGenerator = jsonFactory.createGenerator(out)) {
            jsonGenerator.writeStartArray();

            for (final Object value : values) {
                jsonGenerator.writeObject(value);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.flush();
        }
    }

    private abstract class TextractAction {
        abstract void processFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile) throws IOException;

        void createTextResults(final ProcessContext context, final ProcessSession session, final FlowFile original, final List<Block> blocks,
                               final DocumentMetadata metadata, final String modelVersion) throws IOException {
            final Map<String, String> attributes = new HashMap<>();
            final Integer pages = metadata.getPages();
            if (pages != null) {
                attributes.put("num.pages", pages.toString());
            }

            attributes.put("aws.model.version", modelVersion);
            attributes.put("num.text.blocks", String.valueOf(blocks.size()));

            FlowFile results = session.create(original);
            try (final OutputStream out = session.write(results)) {
                writeAsJson(blocks, out);
            }

            final String textAttributeName = context.getProperty(TEXT_ATTRIBUTE_NAME).getValue();
            if (textAttributeName != null) {
                final int confidenceThreshold = context.getProperty(TEXT_CONFIDENCE_THRESHOLD).asInteger();
                final String text = TextractProperties.lineBlocksToString(blocks, confidenceThreshold, attributes);
                attributes.put(textAttributeName, text);
            }

            // Add attributes to original FlowFile
            session.putAllAttributes(original, attributes);

            // Add attributes to results FlowFile
            attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
            session.putAllAttributes(results, attributes);

            session.transfer(results, REL_ANALYSIS);
            final String transitUri = "https://textract." + context.getProperty(REGION).getValue() + ".amazonaws.com";
            session.getProvenanceReporter().fetch(results, transitUri);
        }
    }

    private class DetectDocumentTextAction extends TextractAction {
        private static final int MAX_FILE_SIZE = 5 * 1024 * 1024;
        private final AmazonTextractAsyncClient client;

        public DetectDocumentTextAction(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) throws IOException {
            final Document document = createDocument(session, flowFile, MAX_FILE_SIZE);

            final DetectDocumentTextRequest request = new DetectDocumentTextRequest();
            request.setDocument(document);

            final DetectDocumentTextResult result = client.detectDocumentText(request);
            createTextResults(context, session, flowFile, result.getBlocks(), result.getDocumentMetadata(), result.getDetectDocumentTextModelVersion());
        }
    }

    private class AnalyzeDocumentAction extends TextractAction {
        private static final int MAX_FILE_SIZE = 10 * 1024 * 1024;
        private static final String TABLES = "TABLES";
        private static final String FORMS = "FORMS";

        private final AmazonTextractAsyncClient client;

        public AnalyzeDocumentAction(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) throws IOException {
            final String analysisFeatures = context.getProperty(TextractProperties.ANALYSIS_FEATURES).evaluateAttributeExpressions(flowFile).getValue();
            final List<String> features;
            if (analysisFeatures.equalsIgnoreCase(TextractProperties.ANALYZE_TABLES.getValue())) {
                features = Collections.singletonList(TABLES);
            } else if (analysisFeatures.equalsIgnoreCase(TextractProperties.ANALYZE_FORMS.getValue())) {
                features = Collections.singletonList(FORMS);
            } else {
                features = Arrays.asList(TABLES, FORMS);
            }

            final Document document = createDocument(session, flowFile, MAX_FILE_SIZE);

            final AnalyzeDocumentRequest request = new AnalyzeDocumentRequest();
            request.setFeatureTypes(features);
            request.setDocument(document);

            final AnalyzeDocumentResult result = client.analyzeDocument(request);
            createTextResults(context, session, flowFile, result.getBlocks(), result.getDocumentMetadata(), result.getAnalyzeDocumentModelVersion());
        }
    }

    private class AnalyzeExpenseAction extends TextractAction {
        private final int MAX_FILE_SIZE = 10 * 1024 * 1024;
        private final AmazonTextractAsyncClient client;

        public AnalyzeExpenseAction(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) throws IOException {
            final Document document = createDocument(session, flowFile, MAX_FILE_SIZE);

            final AnalyzeExpenseRequest request = new AnalyzeExpenseRequest();
            request.setDocument(document);

            final AnalyzeExpenseResult result = client.analyzeExpense(request);
            final List<ExpenseDocument> expenseDocuments = result.getExpenseDocuments();
            final DocumentMetadata documentMetadata = result.getDocumentMetadata();

            final Map<String, String> attributes = new HashMap<>();
            final Integer pages = documentMetadata.getPages();
            if (pages != null) {
                attributes.put("num.pages", pages.toString());
            }

            // Add attributes for line items
            final String lineItemPrefix = context.getProperty(TextractProperties.LINE_ITEM_ATTRIBUTE_PREFIX).getValue();
            if (lineItemPrefix != null) {
                int lineItemNumber = 0;

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
            final String summaryItemPrefix = context.getProperty(TextractProperties.SUMMARY_ITEM_ATTRIBUTE_PREFIX).getValue();
            if (summaryItemPrefix != null) {
                for (final ExpenseDocument expenseDocument : expenseDocuments) {
                    for (final ExpenseField summaryField : expenseDocument.getSummaryFields()) {
                        addExpenseField(summaryField, attributes, summaryItemPrefix);
                    }
                }
            }

            // Write JSON contents to FlowFile
            FlowFile resultFlowFile = session.create(flowFile);
            try (final OutputStream out = session.write(resultFlowFile)) {
                writeAsJson(expenseDocuments, out);
            }

            // Add attributes to FlowFiles and transfer result to analysis relationship
            session.putAllAttributes(flowFile, attributes);

            attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
            resultFlowFile = session.putAllAttributes(resultFlowFile, attributes);

            session.transfer(resultFlowFile, REL_ANALYSIS);

            final String transitUri = "https://textract." + context.getProperty(REGION).getValue() + ".amazonaws.com";
            session.getProvenanceReporter().fetch(resultFlowFile, transitUri);
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
    }

    private class AnalyzeIDAction extends TextractAction {
        private final int MAX_FILE_SIZE = 10 * 1024 * 1024;
        private final AmazonTextractAsyncClient client;

        public AnalyzeIDAction(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public void processFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) throws IOException {
            final Document document = createDocument(session, flowFile, MAX_FILE_SIZE);

            final AnalyzeIDRequest request = new AnalyzeIDRequest();
            request.setDocumentPages(Collections.singletonList(document));

            final AnalyzeIDResult result = client.analyzeID(request);
            final DocumentMetadata documentMetadata = result.getDocumentMetadata();
            final String modelVersion = result.getAnalyzeIDModelVersion();
            final List<IdentityDocument> documents = result.getIdentityDocuments();

            final Map<String, String> attributes = new HashMap<>();
            final Integer pages = documentMetadata.getPages();
            if (pages != null) {
                attributes.put("num.pages", pages.toString());
            }

            attributes.put("aws.model.version", modelVersion);

            FlowFile resultFlowFile = session.create(flowFile);
            try (final OutputStream out = session.write(resultFlowFile)) {
                writeAsJson(documents, out);
            }

            final String attributeNamePrefix = context.getProperty(ID_ATTRIBUTE_PREFIX).getValue();
            if (attributeNamePrefix != null) {
                for (final IdentityDocument identityDocument : documents) {
                    final List<IdentityDocumentField> fields = identityDocument.getIdentityDocumentFields();
                    for (final IdentityDocumentField field : fields) {
                        final String fieldName = field.getType().getText();
                        final String fieldValue = field.getValueDetection().getText();
                        attributes.put(fieldName, fieldValue);
                    }
                }
            }

            session.putAllAttributes(flowFile, attributes);

            attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
            resultFlowFile = session.putAllAttributes(resultFlowFile, attributes);

            session.transfer(resultFlowFile, REL_ANALYSIS);

            final String transitUri = "https://textract." + context.getProperty(REGION).getValue() + ".amazonaws.com";
            session.getProvenanceReporter().fetch(resultFlowFile, transitUri);
        }
    }
}
