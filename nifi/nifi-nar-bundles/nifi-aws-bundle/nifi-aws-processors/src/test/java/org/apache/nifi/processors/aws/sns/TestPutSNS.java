package org.apache.nifi.processors.aws.sns;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestPutSNS {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    
    @Test
    public void testPublish() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSNS());
        runner.setProperty(PutSNS.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutSNS.ARN, "arn:aws:sns:us-west-2:100515378163:test-topic-1");
        assertTrue( runner.setProperty("DynamicProperty", "hello!").isValid() );
        
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run();
        
        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }

}
