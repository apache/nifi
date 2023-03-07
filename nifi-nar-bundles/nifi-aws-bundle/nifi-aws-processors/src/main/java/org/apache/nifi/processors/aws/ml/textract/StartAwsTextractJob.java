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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.StartDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.StartDocumentAnalysisResult;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.StartExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.StartExpenseAnalysisResult;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStarter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Trigger a AWS Textract job. It should be followed by GetAwsTextractJobStatus processor in order to monitor job status.")
@SeeAlso({GetAwsTextractJobStatus.class})
public class StartAwsTextractJob extends AwsMachineLearningJobStarter<AmazonTextractClient, AmazonWebServiceRequest, AmazonWebServiceResult> {
    public static final Validator TEXTRACT_TYPE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            } else if (TextractType.TEXTRACT_TYPES.contains(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Supported Value.").valid(true).build();
            } else {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Not a supported value, flow file attribute or context parameter.").valid(false).build();
            }
        }
    };
    public static final PropertyDescriptor TEXTRACT_TYPE = new PropertyDescriptor.Builder()
            .name("textract-type")
            .displayName("Textract Type")
            .required(true)
            .description("Supported values: \"Document Analysis\", \"Document Text Detection\", \"Expense Analysis\"")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(DOCUMENT_ANALYSIS.type)
            .addValidator(TEXTRACT_TYPE_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> TEXTRACT_PROPERTIES =
        Collections.unmodifiableList(Stream.concat(PROPERTIES.stream(), Stream.of(TEXTRACT_TYPE)).collect(Collectors.toList()));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return TEXTRACT_PROPERTIES;
    }

    @Override
    protected void postProcessFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, AmazonWebServiceResult response) {
        super.postProcessFlowFile(context, session, flowFile, response);
    }

    @Override
    protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTextractClient) AmazonTextractClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    protected AmazonWebServiceResult sendRequest(AmazonWebServiceRequest request, ProcessContext context, FlowFile flowFile) {
        TextractType textractType =
                TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).evaluateAttributeExpressions(flowFile).getValue());
        AmazonWebServiceResult result;
        switch (textractType) {
            case DOCUMENT_ANALYSIS :
                result = getClient(context).startDocumentAnalysis((StartDocumentAnalysisRequest) request);
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = getClient(context).startDocumentTextDetection((StartDocumentTextDetectionRequest) request);
                break;
            case EXPENSE_ANALYSIS:
                result = getClient(context).startExpenseAnalysis((StartExpenseAnalysisRequest) request);
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type: " + textractType);
        }
        return result;
    }

    @Override
    protected Class<? extends AmazonWebServiceRequest> getAwsRequestClass(ProcessContext context, FlowFile flowFile) {
        TextractType typeOfTextract =
                TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).evaluateAttributeExpressions(flowFile).getValue());
        Class<? extends AmazonWebServiceRequest> result = null;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS:
                result = StartDocumentAnalysisRequest.class;
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = StartDocumentTextDetectionRequest.class;
                break;
            case EXPENSE_ANALYSIS:
                result = StartExpenseAnalysisRequest.class;
                break;
        }
        return result;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, AmazonWebServiceResult amazonWebServiceResult, FlowFile flowFile) {
        TextractType textractType =
                TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).evaluateAttributeExpressions(flowFile).getValue());
        String  result;
        switch (textractType) {
            case DOCUMENT_ANALYSIS:
                result = ((StartDocumentAnalysisResult) amazonWebServiceResult).getJobId();
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = ((StartDocumentTextDetectionResult) amazonWebServiceResult).getJobId();
                break;
            case EXPENSE_ANALYSIS:
                result = ((StartExpenseAnalysisResult) amazonWebServiceResult).getJobId();
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type.");
        }
        return result;
    }
}
