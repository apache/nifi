package org.apache.nifi.processors.aws.dynamodb;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ITDeleteDynamoDBTest extends ITAbstractDynamoDBTest {

    @Test
    public void testStringHashStringRangeDeleteOnlyHashFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttribute(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangeDeleteNoHashValueFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeValueNoRangeNameFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeNameNoRangeValueFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentHashSuccess() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "nonexistent");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentRangeSuccess() {
        final TestRunner getRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "nonexistent");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

    }
}
