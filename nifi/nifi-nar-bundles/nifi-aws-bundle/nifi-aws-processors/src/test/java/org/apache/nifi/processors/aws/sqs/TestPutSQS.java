package org.apache.nifi.processors.aws.sqs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.aws.sns.PutSNS;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestPutSQS {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Test
    public void testSimplePut() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSQS());
        runner.setProperty(PutSNS.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutSQS.TIMEOUT, "30 secs");
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/100515378163/test-queue-000000000");
        Assert.assertTrue( runner.setProperty("x-custom-prop", "hello").isValid() );
        
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run(1);
        
        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

}
