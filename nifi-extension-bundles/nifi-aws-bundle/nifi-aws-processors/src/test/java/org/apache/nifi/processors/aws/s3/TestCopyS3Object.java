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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCopyS3Object {
    private TestRunner runner;

    private S3Client mockS3Client;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        final CopyS3Object mockCopyS3Object = new CopyS3Object() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                HeadObjectResponse response = HeadObjectResponse.builder()
                        .contentLength(1000L)
                        .build();

                when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(response);

                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockCopyS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @DisplayName("Test a normal run that SHOULD succeed")
    @Test
    void testRun() {
        runner.enqueue("".getBytes(StandardCharsets.UTF_8), setupRun());
        runner.run();

        runner.assertTransferCount(CopyS3Object.REL_SUCCESS, 1);

        verify(mockS3Client, times(1))
                .copyObject(any(CopyObjectRequest.class));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
    }

    @DisplayName("Validate that S3 errors cleanly route to failure")
    @Test
    void testS3ErrorHandling() {
        final AwsServiceException exception = S3Exception.builder()
                .message("Manually triggered error")
                .statusCode(503)
                .build();
        when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                .thenThrow(exception);

        runner.enqueue(new byte[]{}, setupRun());
        runner.run();

        runner.assertTransferCount(CopyS3Object.REL_FAILURE, 1);
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.of("canned-acl", AbstractS3Processor.CANNED_ACL.getName());

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }

    private Map<String, String> setupRun() {
        runner.setProperty(CopyS3Object.SOURCE_BUCKET, "${s3.bucket.source}");
        runner.setProperty(CopyS3Object.SOURCE_KEY, "${s3.key.source}");
        runner.setProperty(CopyS3Object.DESTINATION_BUCKET, "${s3.bucket.target}");
        runner.setProperty(CopyS3Object.DESTINATION_KEY, "${s3.key.target}");

        return Map.of(
                "s3.bucket.source", "dev-bucket",
                "s3.key.source", "/test.txt",
                "s3.bucket.target", "staging-bucket",
                "s3.key.target", "/copied.txt"
        );
    }
}
