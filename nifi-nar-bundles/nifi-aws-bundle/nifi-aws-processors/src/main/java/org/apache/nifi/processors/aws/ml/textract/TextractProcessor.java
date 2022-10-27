package org.apache.nifi.processors.aws.ml.textract;

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
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.ml.AwsMlProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Automatically extract printed text, handwriting, and data from any document")
@SeeAlso({TextractFetcher.class})
public class TextractProcessor extends AwsMlProcessor<AmazonTextractClient, AmazonWebServiceRequest, AmazonWebServiceResult> {
    private static final String DOCUMENT_ANALYSIS = "Document Analysis";
    private static final String DOCUMENT_TEXT_DETECTION = "Document Text Detection";
    private static final String EXPENSE_ANALYSIS = "Expense Analysis";
    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("type-of-service")
            .displayName("Type of textract")
            .allowableValues(DOCUMENT_ANALYSIS, DOCUMENT_TEXT_DETECTION, EXPENSE_ANALYSIS)
            .required(true)
            .defaultValue("Document Analysis")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ImmutableList.Builder().add(TYPE).add(super.getSupportedPropertyDescriptors().toArray()).build();
    }

    @Override
    protected void postProcessFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, AmazonWebServiceResult response) {
        super.postProcessFlowFile(context, session, flowFile, response);
        session.putAttribute(flowFile, TYPE.getName(), context.getProperty(TYPE.getName()).getValue());
    }

    @Override
    protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTextractClient) AmazonTextractClient.builder().build();
    }

    @Override
    protected AmazonWebServiceResult sendRequest(AmazonWebServiceRequest request, ProcessContext context) {
        String typeOfTextract = context.getProperty(TYPE.getName()).getValue();
        AmazonWebServiceResult result;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS :
                result = getClient().startDocumentAnalysis((StartDocumentAnalysisRequest) request);
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = getClient().startDocumentTextDetection((StartDocumentTextDetectionRequest) request);
                break;
            case EXPENSE_ANALYSIS :
                result = getClient().startExpenseAnalysis((StartExpenseAnalysisRequest) request);
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type.");
        }
        return result;
    }

    @Override
    protected Class<? extends AmazonWebServiceRequest> getAwsRequestClass(ProcessContext context) {
        String typeOfTextract = context.getProperty(TYPE.getName()).getValue();
        Class<? extends AmazonWebServiceRequest>  result;
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
            default: throw new UnsupportedOperationException("Unsupported textract type.");
        }
        return result;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, AmazonWebServiceResult amazonWebServiceResult) {
        String typeOfTextract = context.getProperty(TYPE.getName()).getValue();
        String  result;
        switch (typeOfTextract) {
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
