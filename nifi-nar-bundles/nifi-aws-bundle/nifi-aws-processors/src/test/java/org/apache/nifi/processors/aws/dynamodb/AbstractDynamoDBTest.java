/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.dynamodb;

import org.apache.nifi.util.MockFlowFile;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;

/**
 * Provides reused elements and utilities for the AWS DynamoDB related tests
 *
 * @see GetDynamoDBTest
 * @see PutDynamoDBTest
 * @see DeleteDynamoDBTest
 */
public abstract class AbstractDynamoDBTest {
    public static final String REGION = "us-west-2";
    public static final String stringHashStringRangeTableName = "StringHashStringRangeTable";

    private static final List<String> errorAttributes = List.of(
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_CODE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_MESSAGE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_SERVICE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_REQUEST_ID,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE,
            AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE
    );

    protected DynamoDbClient client;

    protected AwsServiceException getSampleAwsServiceException() {
        final AwsServiceException testServiceException = AwsServiceException.builder()
                .message("Test AWS Service Exception")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("8673509")
                        .errorMessage("This request cannot be serviced right now.")
                        .serviceName("Dynamo DB")
                        .build())
                .requestId("TestRequestId-1234567890")
                .build();

        return testServiceException;
    }

    protected static void validateServiceExceptionAttributes(final MockFlowFile flowFile) {
        errorAttributes.forEach(flowFile::assertAttributeExists);
    }

    protected static AttributeValue string(final String s) {
        return AttributeValue.builder().s(s).build();
    }
}
