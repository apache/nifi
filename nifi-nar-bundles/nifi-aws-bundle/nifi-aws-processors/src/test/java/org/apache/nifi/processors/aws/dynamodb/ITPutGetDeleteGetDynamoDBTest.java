package org.apache.nifi.processors.aws.dynamodb;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ITPutGetDeleteGetDynamoDBTest extends ITAbstractDynamoDBTest {

	@Test
	public void testStringHashStringRangePutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(PutDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(PutDynamoDB.REGION, REGION);
        putRunner.setProperty(PutDynamoDB.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());
        
        putRunner.run(1);
        
        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(GetDynamoDB.REGION, REGION);
        getRunner.setProperty(GetDynamoDB.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_VALUE, "r1");
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});
        
        getRunner.run(1);
        
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(DeleteDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(DeleteDynamoDB.REGION, REGION);
        deleteRunner.setProperty(DeleteDynamoDB.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(DeleteDynamoDB.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_VALUE, "h1");
        deleteRunner.setProperty(DeleteDynamoDB.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});
        
        deleteRunner.run(1);
        
        deleteRunner.assertAllFlowFilesTransferred(DeleteDynamoDB.REL_SUCCESS, 1);

        flowFiles = deleteRunner.getFlowFilesForRelationship(DeleteDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals("", new String(flowFile.toByteArray()));
        }
        
	    // Final check after delete
        final TestRunner getRunnerAfterDelete = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunnerAfterDelete.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(GetDynamoDB.REGION, REGION);
        getRunnerAfterDelete.setProperty(GetDynamoDB.TABLE, stringHashStringRangeTableName);
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashS");
        getRunnerAfterDelete.setProperty(GetDynamoDB.RANGE_KEY_NAME, "rangeS");
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_VALUE, "h1");
        getRunnerAfterDelete.setProperty(GetDynamoDB.RANGE_KEY_VALUE, "r1");
        getRunnerAfterDelete.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.enqueue(new byte[] {});
        
        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_FAILURE, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
        	String error = flowFile.getAttribute(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND);
        	assertTrue(error.startsWith(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }
        
	}

	@Test
	public void testStringHashStringRangePutDeleteWithHashOnlyFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(PutDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(PutDynamoDB.REGION, REGION);
        putRunner.setProperty(PutDynamoDB.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());
        
        putRunner.run(1);
        
        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(GetDynamoDB.REGION, REGION);
        getRunner.setProperty(GetDynamoDB.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_VALUE, "r1");
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});
        
        getRunner.run(1);
        
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(DeleteDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(DeleteDynamoDB.REGION, REGION);
        deleteRunner.setProperty(DeleteDynamoDB.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_VALUE, "h1");
        deleteRunner.enqueue(new byte[] {});
        
        deleteRunner.run(1);
        
        deleteRunner.assertAllFlowFilesTransferred(DeleteDynamoDB.REL_FAILURE, 1);

        flowFiles = deleteRunner.getFlowFilesForRelationship(DeleteDynamoDB.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
        	validateServiceExceptionAttribute(flowFile);
            assertEquals("", new String(flowFile.toByteArray()));
        }
        
	}

	@Test
	public void testStringHashStringRangePutGetWithHashOnlyKeyFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(PutDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(PutDynamoDB.REGION, REGION);
        putRunner.setProperty(PutDynamoDB.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());
        
        putRunner.run(1);
        
        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(GetDynamoDB.REGION, REGION);
        getRunner.setProperty(GetDynamoDB.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});
        
        getRunner.run(1);
        
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_FAILURE, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
        	validateServiceExceptionAttribute(flowFile);
            assertEquals("", new String(flowFile.toByteArray()));
        }

        
	}

	@Test
	public void testNumberHashOnlyPutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(PutDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(PutDynamoDB.REGION, REGION);
        putRunner.setProperty(PutDynamoDB.TABLE, numberHashOnlyTableName);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, "hashN");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE, "40");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, "document");
        String document = "{\"age\":40}";
        putRunner.enqueue(document.getBytes());
        
        putRunner.run(1);
        
        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(GetDynamoDB.REGION, REGION);
        getRunner.setProperty(GetDynamoDB.TABLE, numberHashOnlyTableName);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashN");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE, "40");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});
        
        getRunner.run(1);
        
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(DeleteDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(DeleteDynamoDB.REGION, REGION);
        deleteRunner.setProperty(DeleteDynamoDB.TABLE, numberHashOnlyTableName);
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_NAME, "hashN");
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_VALUE, "40");
        deleteRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        deleteRunner.enqueue(new byte[] {});
        
        deleteRunner.run(1);
        
        deleteRunner.assertAllFlowFilesTransferred(DeleteDynamoDB.REL_SUCCESS, 1);

        flowFiles = deleteRunner.getFlowFilesForRelationship(DeleteDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals("", new String(flowFile.toByteArray()));
        }
        
	    // Final check after delete
        final TestRunner getRunnerAfterDelete = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunnerAfterDelete.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(GetDynamoDB.REGION, REGION);
        getRunnerAfterDelete.setProperty(GetDynamoDB.TABLE, numberHashOnlyTableName);
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashN");
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_VALUE, "40");
        getRunnerAfterDelete.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.enqueue(new byte[] {});
        
        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_FAILURE, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
        	String error = flowFile.getAttribute(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND);
        	assertTrue(error.startsWith(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }
        
	}
	
	@Test
	public void testNumberHashNumberRangePutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(PutDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(PutDynamoDB.REGION, REGION);
        putRunner.setProperty(PutDynamoDB.TABLE, numberHashNumberRangeTableName);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, "hashN");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_NAME, "rangeN");
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE, "40");
        putRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE, "50");
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, "document");
        String document = "{\"40\":\"50\"}";
        putRunner.enqueue(document.getBytes());
        
        putRunner.run(1);
        
        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(GetDynamoDB.REGION, REGION);
        getRunner.setProperty(GetDynamoDB.TABLE, numberHashNumberRangeTableName);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashN");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_NAME, "rangeN");
        getRunner.setProperty(GetDynamoDB.HASH_KEY_VALUE, "40");
        getRunner.setProperty(GetDynamoDB.RANGE_KEY_VALUE, "50");
        getRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});
        
        getRunner.run(1);
        
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner deleteRunner = TestRunners.newTestRunner(DeleteDynamoDB.class);

        deleteRunner.setProperty(DeleteDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        deleteRunner.setProperty(DeleteDynamoDB.REGION, REGION);
        deleteRunner.setProperty(DeleteDynamoDB.TABLE, numberHashNumberRangeTableName);
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_NAME, "hashN");
        deleteRunner.setProperty(DeleteDynamoDB.RANGE_KEY_NAME, "rangeN");
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_VALUE, "40");
        deleteRunner.setProperty(DeleteDynamoDB.RANGE_KEY_VALUE, "50");
        deleteRunner.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        deleteRunner.setProperty(PutDynamoDB.RANGE_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        deleteRunner.enqueue(new byte[] {});
        
        deleteRunner.run(1);
        
        deleteRunner.assertAllFlowFilesTransferred(DeleteDynamoDB.REL_SUCCESS, 1);

        flowFiles = deleteRunner.getFlowFilesForRelationship(DeleteDynamoDB.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals("", new String(flowFile.toByteArray()));
        }
        
	    // Final check after delete
        final TestRunner getRunnerAfterDelete = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunnerAfterDelete.setProperty(GetDynamoDB.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(GetDynamoDB.REGION, REGION);
        getRunnerAfterDelete.setProperty(GetDynamoDB.TABLE, numberHashNumberRangeTableName);
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_NAME, "hashN");
        getRunnerAfterDelete.setProperty(GetDynamoDB.RANGE_KEY_NAME, "rangeN");
        getRunnerAfterDelete.setProperty(GetDynamoDB.HASH_KEY_VALUE, "40");
        getRunnerAfterDelete.setProperty(GetDynamoDB.RANGE_KEY_VALUE, "50");
        getRunnerAfterDelete.setProperty(GetDynamoDB.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.setProperty(PutDynamoDB.HASH_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.setProperty(PutDynamoDB.RANGE_KEY_VALUE_TYPE, PutDynamoDB.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.enqueue(new byte[] {});
        
        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_FAILURE, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
        	String error = flowFile.getAttribute(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND);
        	assertTrue(error.startsWith(GetDynamoDB.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }
        
	}
}
