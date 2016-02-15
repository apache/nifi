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

import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ITPutDynamoDBTest extends ITAbstractDynamoDBTest {

    @Test
    public void testStringHashStringRangePutOnlyHashFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"hello\": 2}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttribute(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangePutNoHashValueFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        String document = "{\"hello\": 2}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangePutOnlyHashWithRangeValueNoRangeNameFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.enqueue(new byte[] {});

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangePutOnlyHashWithRangeNameNoRangeValueFailure() {
        final TestRunner putRunner = TestRunners.newTestRunner(PutDynamoDB.class);

        putRunner.setProperty(AbstractDynamoDBProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        putRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.enqueue(new byte[] {});

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }
}
