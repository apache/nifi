package org.apache.nifi.processors.aws.ml.textract;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.JobStatus;

public class TextractDocumentAnalysisResultUtil implements AwsTextractTaskAware {

    @Override
    public JobStatus getTaskStatus(AmazonTextractClient client, String awstaskId) {
        return JobStatus.fromValue(client.getDocumentAnalysis(new GetDocumentAnalysisRequest().withJobId(awstaskId)).getJobStatus());
    }

    @Override
    public AmazonWebServiceResult getTask(AmazonTextractClient client, String awstaskId) {
        return client.getDocumentAnalysis(new GetDocumentAnalysisRequest().withJobId(awstaskId));
    }
}
