package org.apache.nifi.processors.aws.ml.textract;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.JobStatus;

public interface AwsTextractTaskAware {
    /**
     * Get job status of textract task.
     * @param client AWS client
     * @param awstaskId id of the task which status will be returned
     * @return status of the task.
     */
    JobStatus getTaskStatus(AmazonTextractClient client, String awstaskId);

    /**
     * Return the AWS textract task
     * @param client  AWS client
     * @param awstaskId awstaskId id of the task which status will be returned
     * @return POJO representing a task
     */
    AmazonWebServiceResult getTask(AmazonTextractClient client, String awstaskId);

}
