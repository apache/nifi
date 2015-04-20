package org.apache.nifi.processors.aws.sqs;

import java.util.List;

import org.apache.nifi.processors.aws.sns.PutSNS;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestGetSQS {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Test
    public void testSimpleGet() {
        final TestRunner runner = TestRunners.newTestRunner(new GetSQS());
        runner.setProperty(PutSNS.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(GetSQS.TIMEOUT, "30 secs");
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/100515378163/test-queue-000000000");
        
        runner.run(1);
        
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        for ( final MockFlowFile mff : flowFiles ) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }
    }

}
