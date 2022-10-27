package org.apache.nifi.processors.aws.ml.textract;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.JobStatus;

public class TextractDocumentExpenseAnalysisUtil implements AwsTextractTaskAware {

    @Override
    public JobStatus getTaskStatus(AmazonTextractClient client, String awsTaskId) {
        return JobStatus.fromValue(client.getExpenseAnalysis(new GetExpenseAnalysisRequest().withJobId(awsTaskId)).getJobStatus());
    }

    @Override
    public AmazonWebServiceResult getTask(AmazonTextractClient client, String awsTaskId) {
        return client.getExpenseAnalysis(new GetExpenseAnalysisRequest().withJobId(awsTaskId));
    }
}
