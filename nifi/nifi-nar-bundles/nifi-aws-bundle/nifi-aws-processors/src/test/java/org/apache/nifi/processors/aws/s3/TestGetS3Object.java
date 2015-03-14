package org.apache.nifi.processors.aws.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestGetS3Object {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    
    @Test
    public void testGet() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new GetS3Object());
        runner.setProperty(GetS3Object.BUCKET, "anonymous-test-bucket-00000000");
        runner.setProperty(GetS3Object.CREDENTAILS_FILE, CREDENTIALS_FILE);
        runner.setProperty(GetS3Object.KEY, "folder/1.txt");
        
        runner.setProperty(GetS3Object.BYTE_RANGE_START, "${start}");
        runner.setProperty(GetS3Object.BYTE_RANGE_END, String.valueOf(Long.MAX_VALUE));
        
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("start", "0");
        
        runner.enqueue(new byte[0], attrs);
        runner.run(1);
        
        runner.assertAllFlowFilesTransferred(GetS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(GetS3Object.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();
        
        final byte[] expectedBytes = Files.readAllBytes(Paths.get("src/test/resources/hello.txt"));
        out.assertContentEquals(new String(expectedBytes));
        for ( final Map.Entry<String, String> entry : out.getAttributes().entrySet() ) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
    
}
