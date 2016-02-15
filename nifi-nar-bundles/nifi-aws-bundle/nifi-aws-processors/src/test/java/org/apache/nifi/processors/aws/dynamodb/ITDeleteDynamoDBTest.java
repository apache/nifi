package org.apache.nifi.processors.aws.dynamodb;


import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ITDeleteDynamoDBTest extends ITAbstractDynamoDBTest {

    @Test
    public void testStringHashStringRangeDeleteOnlyHashFailure() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttribute(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangeDeleteNoHashValueFailure() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeValueNoRangeNameFailure() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeNameNoRangeValueFailure() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentHashSuccess() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "nonexistent");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentRangeSuccess() {
        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "nonexistent");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

    }
}
