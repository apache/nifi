package org.apache.nifi.processors.aws.ml.textract;

import static org.apache.nifi.processors.aws.ml.textract.TextractProcessor.TYPE;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.JobStatus;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMLFetcherProcessor;
import org.apache.nifi.processors.aws.ml.polly.PollyFetcher;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Automatically extract printed text, handwriting, and data from any document")
@SeeAlso({TextractProcessor.class})
public class TextractFetcher extends AwsMLFetcherProcessor<AmazonTextractClient> {
    public static final String DOCUMENT_ANALYSIS = "Document Analysis";
    public static final String DOCUMENT_TEXT_DETECTION = "Document Text Detection";
    public static final String EXPENSE_ANALYSIS = "Expense Analysis";
    private static final Map<String, AwsTextractTaskAware> UTIL_MAPPING =
            ImmutableMap.of(DOCUMENT_ANALYSIS, new TextractDocumentAnalysisResultUtil(),
                    DOCUMENT_TEXT_DETECTION, new TextractDocumentTextDetectionUtil(),
                    EXPENSE_ANALYSIS, new TextractDocumentExpenseAnalysisUtil());

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
        String typeOfTextract = flowFile.getAttribute(TYPE.getName());
        AwsTextractTaskAware util = UTIL_MAPPING.get(typeOfTextract);

        String awsTaskId = flowFile.getAttribute(AWS_TASK_ID_PROPERTY);
        JobStatus jobStatus = util.getTaskStatus(getClient(), awsTaskId);
        if (JobStatus.SUCCEEDED == jobStatus) {
            writeToFlowFile(session, flowFile, util.getTask(getClient(), awsTaskId));
            session.transfer(flowFile, REL_SUCCESS);
        }

        if (JobStatus.IN_PROGRESS == jobStatus) {
            session.transfer(flowFile, REL_IN_PROGRESS);
        }

        if (JobStatus.PARTIAL_SUCCESS == jobStatus) {
            session.transfer(flowFile, REL_PARTIAL_SUCCESS);
        }

        if (JobStatus.FAILED == jobStatus) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Textract reported that the task failed for awsTaskId: {}", awsTaskId);
            return;
        }
    }
}
