/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml.textract;

import static org.apache.nifi.processors.aws.ml.textract.TextractType.DOCUMENT_ANALYSIS;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.JobStatus;
import com.amazonaws.services.textract.model.ThrottlingException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Retrieves the current status of an AWS Textract job.")
@SeeAlso({StartAwsTextractJob.class})
public class GetAwsTextractJobStatus extends AwsMachineLearningJobStatusProcessor<AmazonTextractClient> {
    public static final PropertyDescriptor TEXTRACT_TYPE = new PropertyDescriptor.Builder()
            .name("textract-type")
            .displayName("Textract Type")
            .required(true)
            .description("Supported values: \"Document Analysis\", \"Document Text Detection\", \"Expense Analysis\"")
            .allowableValues(TextractType.TEXTRACT_TYPES)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(DOCUMENT_ANALYSIS.getType())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> TEXTRACT_PROPERTIES =
            Collections.unmodifiableList(Stream.concat(PROPERTIES.stream(), Stream.of(TEXTRACT_TYPE)).collect(Collectors.toList()));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return TEXTRACT_PROPERTIES;
    }

    @Override
    protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTextractClient) AmazonTextractClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String textractType = context.getProperty(TEXTRACT_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        String awsTaskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            JobStatus jobStatus = getTaskStatus(TextractType.fromString(textractType), getClient(context), awsTaskId);
            if (JobStatus.SUCCEEDED == jobStatus) {
                Object task = getTask(TextractType.fromString(textractType), getClient(context), awsTaskId);
                writeToFlowFile(session, flowFile, task);
                session.transfer(flowFile, REL_SUCCESS);
            } else if (JobStatus.IN_PROGRESS == jobStatus) {
                session.transfer(flowFile, REL_RUNNING);
            } else if (JobStatus.PARTIAL_SUCCESS == jobStatus) {
                session.transfer(flowFile, REL_THROTTLED);
            } else if (JobStatus.FAILED == jobStatus) {
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Amazon Textract Task [{}] Failed", awsTaskId);
            }
        } catch (ThrottlingException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            session.transfer(flowFile, REL_THROTTLED);
            return;
        } catch (Exception e) {
            getLogger().warn("Failed to get Textract Job status", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

    private Object getTask(TextractType typeOfTextract, AmazonTextractClient client, String awsTaskId) {
        Object job = null;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS:
                job = client.getDocumentAnalysis(new GetDocumentAnalysisRequest().withJobId(awsTaskId));
                break;
            case DOCUMENT_TEXT_DETECTION:
                job = client.getDocumentTextDetection(new GetDocumentTextDetectionRequest().withJobId(awsTaskId));
                break;
            case EXPENSE_ANALYSIS:
                job = client.getExpenseAnalysis(new GetExpenseAnalysisRequest().withJobId(awsTaskId));
                break;
        }
        return job;
    }

    private JobStatus getTaskStatus(TextractType typeOfTextract, AmazonTextractClient client, String awsTaskId) {
        JobStatus jobStatus = JobStatus.IN_PROGRESS;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS:
                jobStatus = JobStatus.fromValue(client.getDocumentAnalysis(new GetDocumentAnalysisRequest().withJobId(awsTaskId)).getJobStatus());
                break;
            case DOCUMENT_TEXT_DETECTION:
                jobStatus = JobStatus.fromValue(client.getDocumentTextDetection(new GetDocumentTextDetectionRequest().withJobId(awsTaskId)).getJobStatus());
                break;
            case EXPENSE_ANALYSIS:
                jobStatus = JobStatus.fromValue(client.getExpenseAnalysis(new GetExpenseAnalysisRequest().withJobId(awsTaskId)).getJobStatus());
                break;

        }
        return jobStatus;
    }
}
