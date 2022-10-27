package org.apache.nifi.processors.aws.ml.textract;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.JobStatus;

public class TextractDocumentTextDetectionUtil implements AwsTextractTaskAware {

    @Override
    public JobStatus getTaskStatus(AmazonTextractClient client, String awsTaskId) {
        return JobStatus.fromValue(client.getDocumentTextDetection(new GetDocumentTextDetectionRequest().withJobId(awsTaskId)).getJobStatus());
    }

    @Override
    public AmazonWebServiceResult getTask(AmazonTextractClient client, String awsTaskId) {
        return client.getDocumentTextDetection(new GetDocumentTextDetectionRequest().withJobId(awsTaskId));
    }
}
