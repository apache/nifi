package org.apache.nifi.processors.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.regions.Regions;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class TestListS3 {

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private static final String BUCKET = "wildfire-data";
    private static final String PREFIX = "sandbox/twitter/" + new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    private static final String REGION = Regions.US_EAST_1.getName();
    
    private TestRunner buildTestRunner() {
        TestRunner runner = TestRunners.newTestRunner(new ListS3());
        runner.setProperty(ListS3.BUCKET, BUCKET);
        runner.setProperty(ListS3.PREFIX, PREFIX);
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        return runner;
    }
    
    private void assertFlowfiles(List<MockFlowFile> flowFiles) {
//        System.out.println("Flow files: " + flowFiles.size());
        for (MockFlowFile f : flowFiles) {
            assertNotNull(f.getAttribute("s3.bucket"));
            assertNotNull(f.getAttribute("filename"));
            assertNotNull(f.getAttribute("s3.owner"));
            assertNotNull(f.getAttribute("s3.lastModified"));
            assertNotNull(f.getAttribute("s3.length"));
            assertNotNull(f.getAttribute("s3.storageClass"));
            
//            System.out.println(f.getAttribute("s3.bucket") + ":" + f.getAttribute("filename")
//            + ":" + f.getAttribute("s3.owner") + ":" + f.getAttribute("s3.lastModified")
//            + ":" + f.getAttribute("s3.length")  + ":" + f.getAttribute("s3.storageClass"));
        }
    }
    
    @Test
    public void testList() throws IOException {
        TestRunner runner = buildTestRunner();

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
        assertFlowfiles(flowFiles);
    }
    
    @Test
    public void testListWithMax() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MAX_OBJECTS, "3");

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertEquals(3, flowFiles.size());
        assertFlowfiles(flowFiles);
    }
    
    @Test
    public void testListDateRange() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MIN_AGE, "4 hr");
        runner.setProperty(ListS3.MAX_AGE, "5 hr");

        runner.enqueue(new byte[0]);
        runner.run(1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());
        assertFlowfiles(flowFiles);
    }
    
    @Test
    public void testInvalidDateRange() throws IOException {
        TestRunner runner = buildTestRunner();
        runner.setProperty(ListS3.MIN_AGE, "4 hr");
        runner.setProperty(ListS3.MAX_AGE, "1 hr");
        
        Collection<ValidationResult> results = new HashSet<>();
        runner.enqueue(new byte[0]);
        ProcessContext pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Minimum Age cannot be greater than Maximum Age"));
        }
    }
}
