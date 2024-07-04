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

package org.apache.nifi.processors.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetS3ObjectMetadata {
    private TestRunner runner = null;
    private GetS3ObjectMetadata mockGetS3ObjectMetadata = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3Client.class);
        mockGetS3ObjectMetadata = new GetS3ObjectMetadata() {
            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region, final ClientConfiguration config,
                                                  final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockGetS3ObjectMetadata);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    private void commonTest() {
        runner.setProperty(GetS3ObjectMetadata.BUCKET_WITH_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(GetS3ObjectMetadata.KEY, "${filename}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt"));

        runner.run();
    }

    @Test
    public void testRunExists() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenReturn(true);
        commonTest();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
    }

    @Test
    public void testRunDoesNotExist() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenReturn(false);
        commonTest();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    @Test
    public void testRunHasS3Error() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenThrow(new RuntimeException("Manually triggered error"));
        commonTest();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }
}
