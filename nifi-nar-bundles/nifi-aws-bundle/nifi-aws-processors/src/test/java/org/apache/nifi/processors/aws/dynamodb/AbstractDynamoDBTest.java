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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;

/**
 * Provides reused elements and utilities for the AWS DynamoDB related tests
 *
 * @see GetDynamoDBTest
 * @see PutDynamoDBTest
 * @see DeleteDynamoDBTest
 */
public abstract class AbstractDynamoDBTest {

    protected AmazonServiceException getSampleAwsServiceException() {
        final AmazonServiceException testServiceException = new AmazonServiceException("Test AWS Service Exception");
        testServiceException.setErrorCode("8673509");
        testServiceException.setErrorMessage("This request cannot be serviced right now.");
        testServiceException.setErrorType(ErrorType.Service);
        testServiceException.setServiceName("Dynamo DB");
        testServiceException.setRequestId("TestRequestId-1234567890");

        return testServiceException;
    }
}
