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

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.JobStatus;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Retrieves the current status of an AWS Textract job.")
@SeeAlso({StartAwsTextractJob.class})
public class GetAwsTextractJobStatus extends AwsMLJobStatusGetter<AmazonTextractClient> {
    public static final String DOCUMENT_ANALYSIS = "Document Analysis";
    public static final String DOCUMENT_TEXT_DETECTION = "Document Text Detection";
    public static final String EXPENSE_ANALYSIS = "Expense Analysis";
    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("type-of-service")
            .displayName("Type of textract")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .allowableValues(DOCUMENT_ANALYSIS, DOCUMENT_TEXT_DETECTION, EXPENSE_ANALYSIS)
            .required(true)
            .defaultValue("Document Analysis")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> TEXTRACT_PROPERTIES = ImmutableList.<PropertyDescriptor>builder()
            .addAll(PROPERTIES)
            .add(TYPE)
            .build();;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return TEXTRACT_PROPERTIES;
    }

    @Override
    protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTextractClient) AmazonTextractClient.builder().build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String typeOfTextract = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();

        String awsTaskId = flowFile.getAttribute(AWS_TASK_ID_PROPERTY);
        JobStatus jobStatus = getTaskStatus(typeOfTextract, getClient(), awsTaskId);
        if (JobStatus.SUCCEEDED == jobStatus) {
            writeToFlowFile(session, flowFile, getTask(typeOfTextract, getClient(), awsTaskId));
            session.transfer(flowFile, REL_SUCCESS);
        }

        if (JobStatus.IN_PROGRESS == jobStatus) {
            session.transfer(flowFile, REL_IN_PROGRESS);
        }

        if (JobStatus.PARTIAL_SUCCESS == jobStatus) {
            session.transfer(flowFile, REL_THROTTLED);
        }

        if (JobStatus.FAILED == jobStatus) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Textract reported that the task failed for awsTaskId: {}", awsTaskId);
            return;
        }
    }

    private Object getTask(String typeOfTextract, AmazonTextractClient client, String awsTaskId) {
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

    private JobStatus getTaskStatus(String typeOfTextract, AmazonTextractClient client, String awsTaskId) {
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
