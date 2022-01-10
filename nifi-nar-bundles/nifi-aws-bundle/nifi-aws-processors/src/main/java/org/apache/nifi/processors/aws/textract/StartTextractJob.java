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
import com.amazonaws.services.textract.model.BadDocumentException;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.DocumentTooLargeException;
import com.amazonaws.services.textract.model.HumanLoopQuotaExceededException;
import com.amazonaws.services.textract.model.IdempotentParameterMismatchException;
import com.amazonaws.services.textract.model.InternalServerErrorException;
import com.amazonaws.services.textract.model.InvalidKMSKeyException;
import com.amazonaws.services.textract.model.InvalidParameterException;
import com.amazonaws.services.textract.model.InvalidS3ObjectException;
import com.amazonaws.services.textract.model.NotificationChannel;
import com.amazonaws.services.textract.model.OutputConfig;
import com.amazonaws.services.textract.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.StartDocumentAnalysisResult;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.StartExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.StartExpenseAnalysisResult;
import com.amazonaws.services.textract.model.ThrottlingException;
import com.amazonaws.services.textract.model.UnsupportedDocumentException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;

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

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

@CapabilityDescription("Starts a Textract Job to analyze and extract information from an image or PDF file that is stored in Amazon S3. This processor initiates the processing but does not wait for" +
    " the result. Instead, it adds attributes indicating the identifier of the Textract Job and the top of job that it was. See Additional Details for more information on the expected usage of this" +
    " Processor and how it can interact with other processors to obtain the desired result.")
@WritesAttributes({
    @WritesAttribute(attribute = "textract.job.id", description = "The ID of the textract job"),
    @WritesAttribute(attribute = "textract.action", description = "The type of action being performed")
})
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"aws", "cloud", "text", "textract", "unstructured", "detect", "analyze"})
@SeeAlso({AWSCredentialsProviderService.class, FetchTextractResults.class})
public class StartTextractJob extends AbstractAWSCredentialsProviderProcessor<AmazonTextractAsyncClient> {
    private static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";

    static final PropertyDescriptor DOCUMENT_S3_BUCKET = new PropertyDescriptor.Builder()
        .name("Document S3 Bucket")
        .displayName("Document S3 Bucket")
        .description("The name of the S3 Bucket that contains the document")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("${s3.bucket}")
        .build();
    static final PropertyDescriptor DOCUMENT_S3_NAME = new PropertyDescriptor.Builder()
        .name("Document Object Name")
        .displayName("Document Object Name")
        .description("The name of the document in the S3 bucket to perform analysis against")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("${filename}")
        .build();
    static final PropertyDescriptor DOCUMENT_VERSION = new PropertyDescriptor.Builder()
        .name("Document Version")
        .displayName("Document Version")
        .description("The version of the document in the S3 bucket to perform analysis against")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor CLIENT_REQUEST_TOKEN = new PropertyDescriptor.Builder()
        .name("Client Request Token")
        .displayName("Client Request Token")
        .description("A value that will be sent to AWS in order to uniquely identify the request. This prevents accidental job duplication, which could lead to unexpected expenses. For example, if " +
            "NiFi is restarted and the same FlowFile is sent a second time, this will result in AWS Textract being smart enough not to process the data again. However, if the Processor settings " +
            "change, this could result in a processing failure. In such a case, this value may need to be changed.")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("${uuid}")
        .build();
    static final PropertyDescriptor JOB_TAG = new PropertyDescriptor.Builder()
        .name("Job Tag")
        .displayName("Job Tag")
        .description("The Job Tag to include in the request to AWS. This can be used to identify jobs in the completion statuses that are published to Amazon SNS.")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor KMS_KEY_ID = new PropertyDescriptor.Builder()
        .name("KMS Key ID")
        .displayName("KMS Key ID")
        .description("If specified, the key whose ID is provided will be used to encrypt the results before writing the results to S3")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor SNS_TOPIC_ARN = new PropertyDescriptor.Builder()
        .name("SNS Topic ARN")
        .displayName("SNS Topic ARN")
        .description("When Textract completes processing of the document, it will place a notification onto the topic with the specified ARN")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor ROLE_ARN = new PropertyDescriptor.Builder()
        .name("Role ARN")
        .displayName("Role ARN")
        .description("The ARN of the Amazon Role that Textract is to use in order to publish results to the configured SNS Topic")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor OUTPUT_S3_BUCKET = new PropertyDescriptor.Builder()
        .name("Output S3 Bucket")
        .displayName("Output S3 Bucket")
        .description("The name of the S3 Bucket that the Textract output should be written to")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor OUTPUT_S3_PREFIX = new PropertyDescriptor.Builder()
        .name("Output S3 Prefix")
        .displayName("Output S3 Prefix")
        .description("The prefix for Textract to use when naming the output file in the configured Output S3 Bucket")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        TextractProperties.ACTION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        DOCUMENT_S3_BUCKET,
        DOCUMENT_S3_NAME,
        DOCUMENT_VERSION,
        OUTPUT_S3_BUCKET,
        OUTPUT_S3_PREFIX,
        ROLE_ARN,
        SNS_TOPIC_ARN,
        TextractProperties.ANALYSIS_FEATURES,
        REGION,
        CLIENT_REQUEST_TOKEN,
        JOB_TAG,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD));


    static final Relationship REL_INVALID_INPUT = new Relationship.Builder()
        .name("invalid input")
        .description("AWS Textract indicated that the input was not valid for some reason - for instance, if the document cannot be found in S3.")
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
        REL_SUCCESS,
        REL_INVALID_INPUT,
        REL_TRANSIENT_FAILURE,
        REL_FAILURE)));

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
        final JobTrigger jobTrigger = getJobTrigger(actionName);
        if (jobTrigger == null) {
            getLogger().error("Cannot analyze text from {} because the Textract Action provided is invalid: {}", flowFile, actionName);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            final String jobId = jobTrigger.startJob(context, flowFile);

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("textract.job.id", jobId);
            attributes.put("textract.action", actionName);

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            final String transitUri = "https://textract." + context.getProperty(REGION).getValue() + ".amazonaws.com/jobs/" + jobId;
            session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUri);
        } catch (final InterruptedException ie) {
            getLogger().error("Interrupted while waiting for Textract job to start for {}. Will transfer to failure", flowFile, ie);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final BadDocumentException | DocumentTooLargeException | InvalidParameterException | UnsupportedDocumentException | InvalidKMSKeyException | InvalidS3ObjectException e) {
            getLogger().error("Failed to start job for {}. Routing to {}", flowFile, REL_INVALID_INPUT.getName(), e);
            flowFile = session.penalize(flowFile);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_INVALID_INPUT);
        } catch (final IdempotentParameterMismatchException e) {
            getLogger().error("Failed to start job for {} because a job has already started for this document but with different parameters. If you would like to restart the job, you must provide a" +
                " different value for the <{}> property. Routing to {}" + flowFile, CLIENT_REQUEST_TOKEN.getDisplayName(), REL_TRANSIENT_FAILURE.getName(), e);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_TRANSIENT_FAILURE);
            context.yield();
        } catch (final HumanLoopQuotaExceededException | InternalServerErrorException | ProvisionedThroughputExceededException | ThrottlingException e) {
            getLogger().error("Failed to start job for {}. Routing to {}", flowFile, REL_TRANSIENT_FAILURE.getName(), e);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_TRANSIENT_FAILURE);
            context.yield();
        } catch (final Throwable t) {
            getLogger().error("Failed to process {}", flowFile, t);
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, t.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private JobTrigger getJobTrigger(final String actionName) {
        if (actionName.equalsIgnoreCase(TextractProperties.DETECT_TEXT.getValue())) {
            return new DetectDocumentTextTrigger(client);
        }
        if (actionName.equalsIgnoreCase(TextractProperties.ANALYZE_DOCUMENT.getValue())) {
            return new AnalyzeDocumentTrigger(client);
        }
        if (actionName.equalsIgnoreCase(TextractProperties.ANALYZE_EXPENSE.getValue())) {
            return new AnalyzeExpenseTrigger(client);
        }

        return null;
    }

    @Override
    protected AmazonTextractAsyncClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return null;
    }

    private DocumentLocation createDocumentLocation(final ProcessContext context, final FlowFile flowFile) {
        final S3Object s3Object = new S3Object();
        s3Object.setBucket(context.getProperty(DOCUMENT_S3_BUCKET).evaluateAttributeExpressions(flowFile).getValue());
        s3Object.setName(context.getProperty(DOCUMENT_S3_NAME).evaluateAttributeExpressions(flowFile).getValue());
        s3Object.setVersion(context.getProperty(DOCUMENT_VERSION).evaluateAttributeExpressions(flowFile).getValue());

        final DocumentLocation location = new DocumentLocation();
        location.setS3Object(s3Object);
        return location;
    }

    private NotificationChannel createNotificationChannel(final ProcessContext context, final FlowFile flowFile) {
        final NotificationChannel channel = new NotificationChannel();
        channel.setRoleArn(context.getProperty(ROLE_ARN).evaluateAttributeExpressions(flowFile).getValue());
        channel.setSNSTopicArn(context.getProperty(SNS_TOPIC_ARN).evaluateAttributeExpressions(flowFile).getValue());
        return channel;
    }

    private OutputConfig createOutputConfig(final ProcessContext context, final FlowFile flowFile) {
        final OutputConfig outputConfig = new OutputConfig();
        outputConfig.setS3Bucket(context.getProperty(OUTPUT_S3_BUCKET).evaluateAttributeExpressions(flowFile).getValue());
        outputConfig.setS3Prefix(context.getProperty(OUTPUT_S3_PREFIX).evaluateAttributeExpressions(flowFile).getValue());
        return outputConfig;
    }

    private interface JobTrigger {
        String startJob(ProcessContext context, FlowFile flowFile) throws Throwable;
    }

    private class DetectDocumentTextTrigger implements JobTrigger {
        private final AmazonTextractAsyncClient client;

        public DetectDocumentTextTrigger(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public String startJob(final ProcessContext context, final FlowFile flowFile) throws Throwable {
            final StartDocumentTextDetectionRequest request = new StartDocumentTextDetectionRequest();
            request.setClientRequestToken(context.getProperty(CLIENT_REQUEST_TOKEN).evaluateAttributeExpressions(flowFile).getValue());
            request.setDocumentLocation(createDocumentLocation(context, flowFile));
            request.setJobTag(context.getProperty(JOB_TAG).evaluateAttributeExpressions(flowFile).getValue());
            request.setKMSKeyId(context.getProperty(KMS_KEY_ID).evaluateAttributeExpressions(flowFile).getValue());
            request.setNotificationChannel(createNotificationChannel(context, flowFile));
            request.setOutputConfig(createOutputConfig(context, flowFile));

            final Future<StartDocumentTextDetectionResult> resultFuture = client.startDocumentTextDetectionAsync(request);

            try {
                return resultFuture.get().getJobId();
            } catch (final ExecutionException e) {
                throw e.getCause();
            }
        }
    }

    private class AnalyzeDocumentTrigger implements JobTrigger {
        private static final String TABLES = "TABLES";
        private static final String FORMS = "FORMS";
        private final AmazonTextractAsyncClient client;

        public AnalyzeDocumentTrigger(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public String startJob(final ProcessContext context, final FlowFile flowFile) throws Throwable {
            final StartDocumentAnalysisRequest request = new StartDocumentAnalysisRequest();
            request.setClientRequestToken(context.getProperty(CLIENT_REQUEST_TOKEN).evaluateAttributeExpressions(flowFile).getValue());
            request.setDocumentLocation(createDocumentLocation(context, flowFile));
            request.setJobTag(context.getProperty(JOB_TAG).evaluateAttributeExpressions(flowFile).getValue());
            request.setKMSKeyId(context.getProperty(KMS_KEY_ID).evaluateAttributeExpressions(flowFile).getValue());
            request.setNotificationChannel(createNotificationChannel(context, flowFile));
            request.setOutputConfig(createOutputConfig(context, flowFile));
            request.setFeatureTypes(createFeatureTypes(context, flowFile));

            final Future<StartDocumentAnalysisResult> resultFuture = client.startDocumentAnalysisAsync(request);

            try {
                return resultFuture.get().getJobId();
            } catch (final ExecutionException e) {
                throw e.getCause();
            }
        }

        private Collection<String> createFeatureTypes(final ProcessContext context, final FlowFile flowFile) {
            final String analysisFeatures = context.getProperty(TextractProperties.ANALYSIS_FEATURES).evaluateAttributeExpressions(flowFile).getValue();
            if (analysisFeatures.equalsIgnoreCase(TextractProperties.ANALYZE_TABLES.getValue())) {
                return Collections.singletonList(TABLES);
            } else if (analysisFeatures.equalsIgnoreCase(TextractProperties.ANALYZE_FORMS.getValue())) {
                return Collections.singletonList(FORMS);
            } else {
                return Arrays.asList(TABLES, FORMS);
            }
        }
    }

    private class AnalyzeExpenseTrigger implements JobTrigger {
        private final AmazonTextractAsyncClient client;

        public AnalyzeExpenseTrigger(final AmazonTextractAsyncClient client) {
            this.client = client;
        }

        @Override
        public String startJob(final ProcessContext context, final FlowFile flowFile) throws Throwable {
            final StartExpenseAnalysisRequest request = new StartExpenseAnalysisRequest();
            request.setClientRequestToken(context.getProperty(CLIENT_REQUEST_TOKEN).evaluateAttributeExpressions(flowFile).getValue());
            request.setDocumentLocation(createDocumentLocation(context, flowFile));
            request.setJobTag(context.getProperty(JOB_TAG).evaluateAttributeExpressions(flowFile).getValue());
            request.setKMSKeyId(context.getProperty(KMS_KEY_ID).evaluateAttributeExpressions(flowFile).getValue());
            request.setNotificationChannel(createNotificationChannel(context, flowFile));
            request.setOutputConfig(createOutputConfig(context, flowFile));

            final Future<StartExpenseAnalysisResult> resultFuture = client.startExpenseAnalysisAsync(request);

            try {
                return resultFuture.get().getJobId();
            } catch (final ExecutionException e) {
                throw e.getCause();
            }
        }
    }

}
