package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;


public class TestPenalizeFlowFile {

    @Test
    public void testFlowFilePenalization(){
        final TestRunner runner = TestRunners.newTestRunner(new PenalizeFlowFile());
        runner.enqueue("");
        runner.run();
        runner.assertPenalizeCount(1);
        runner.assertAllFlowFilesTransferred(PenalizeFlowFile.REL_SUCCESS);

        MockFlowFile ff = runner.getFlowFilesForRelationship(PenalizeFlowFile.REL_SUCCESS).get(0);
        Assert.assertTrue(ff.isPenalized());

        runner.clearTransferState();

        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PenalizeFlowFile.REL_SUCCESS, 1);

        ff = runner.getFlowFilesForRelationship(PenalizeFlowFile.REL_SUCCESS).get(0);
        Assert.assertTrue(ff.isPenalized());
    }
}
