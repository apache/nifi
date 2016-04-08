/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.dynamodb;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ITPutGetDeleteGetDynamoDBTest extends ITAbstractDynamoDBTest {


    @Test
    public void testStringHashStringRangePutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractWriteDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractWriteDynamoDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractWriteDynamoDBProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
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

        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.enqueue(new byte[] {});

        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_NOT_FOUND, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_NOT_FOUND);
        for (MockFlowFile flowFile : flowFiles) {
            String error = flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND);
            assertTrue(error.startsWith(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangePutDeleteWithHashOnlyFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractWriteDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractWriteDynamoDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractWriteDynamoDBProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
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

        putRunner.setProperty(AbstractWriteDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractWriteDynamoDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractWriteDynamoDBProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttribute(flowFile);
            assertEquals("", new String(flowFile.toByteArray()));
        }


    }

    @Test
    public void testNumberHashOnlyPutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractWriteDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.TABLE, numberHashOnlyTableName);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE, "40");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"age\":40}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractWriteDynamoDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractWriteDynamoDBProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, numberHashOnlyTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "40");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
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
        deleteRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
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

        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.TABLE, numberHashOnlyTableName);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "40");
        getRunnerAfterDelete.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.enqueue(new byte[] {});

        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_NOT_FOUND, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_NOT_FOUND);
        for (MockFlowFile flowFile : flowFiles) {
            String error = flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND);
            assertTrue(error.startsWith(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }

    }

    @Test
    public void testNumberHashNumberRangePutGetDeleteGetSuccess() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractWriteDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.TABLE, numberHashNumberRangeTableName);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_NAME, "rangeN");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE, "40");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE, "50");
        putRunner.setProperty(AbstractWriteDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"40\":\"50\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractWriteDynamoDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractWriteDynamoDBProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }

        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

        getRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, numberHashNumberRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeN");
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "40");
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "50");
        getRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
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
        deleteRunner.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        deleteRunner.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
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

        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.TABLE, numberHashNumberRangeTableName);
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashN");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeN");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "40");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "50");
        getRunnerAfterDelete.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        getRunnerAfterDelete.setProperty(AbstractWriteDynamoDBProcessor.HASH_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.setProperty(AbstractWriteDynamoDBProcessor.RANGE_KEY_VALUE_TYPE, AbstractWriteDynamoDBProcessor.ALLOWABLE_VALUE_NUMBER);
        getRunnerAfterDelete.enqueue(new byte[] {});

        getRunnerAfterDelete.run(1);
        getRunnerAfterDelete.assertAllFlowFilesTransferred(GetDynamoDB.REL_NOT_FOUND, 1);

        flowFiles = getRunnerAfterDelete.getFlowFilesForRelationship(GetDynamoDB.REL_NOT_FOUND);
        for (MockFlowFile flowFile : flowFiles) {
            String error = flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND);
            assertTrue(error.startsWith(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE));
        }

    }
}
