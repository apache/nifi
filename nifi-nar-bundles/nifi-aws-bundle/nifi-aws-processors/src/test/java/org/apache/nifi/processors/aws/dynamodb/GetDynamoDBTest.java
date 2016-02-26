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
import static org.apache.nifi.processors.aws.dynamodb.ITAbstractDynamoDBTest.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.dynamodb.ITAbstractDynamoDBTest.REGION;
import static org.apache.nifi.processors.aws.dynamodb.ITAbstractDynamoDBTest.stringHashStringRangeTableName;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class GetDynamoDBTest {

    @Test
    public void testStringHashStringRangeGetOnlyHashFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

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
            ITAbstractDynamoDBTest.validateServiceExceptionAttribute(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangeGetNoHashValueFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

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
    public void testStringHashStringRangeGetOnlyHashWithRangeValueNoRangeNameFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

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
    public void testStringHashStringRangeGetOnlyHashWithRangeNameNoRangeValueFailure() {
        final TestRunner getRunner = TestRunners.newTestRunner(GetDynamoDB.class);

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
}
