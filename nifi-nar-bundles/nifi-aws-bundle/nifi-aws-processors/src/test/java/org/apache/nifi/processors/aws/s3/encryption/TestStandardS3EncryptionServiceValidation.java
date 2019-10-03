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
package org.apache.nifi.processors.aws.s3.encryption;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_CSE_C;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_CSE_KMS;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_NONE;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_C;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3;
import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createKey;

public class TestStandardS3EncryptionServiceValidation {

    private TestRunner runner;
    private StandardS3EncryptionService service;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardS3EncryptionService();
        runner.addControllerService("s3-encryption-service", service);
    }


    // NoOpEncryptionStrategy

    @Test
    public void testValidNoOpEncryptionStrategy() {
        configureService(STRATEGY_NAME_NONE, null);

        runner.assertValid(service);
    }

    @Test
    public void testInvalidNoOpEncryptionStrategyBecauseKeyIdOrMaterialSpecified() {
        configureService(STRATEGY_NAME_NONE, "key-id");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidNoOpEncryptionStrategyBecauseKeyIdOrMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_NONE, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidNoOpEncryptionStrategyBecauseKeyIdOrMaterialSpecifiedUsingEL() {
        configureService(STRATEGY_NAME_NONE, "${key-id-var}");

        runner.assertNotValid(service);
    }


    // ServerSideS3EncryptionStrategy

    @Test
    public void testValidServerSideS3EncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_S3, null);

        runner.assertValid(service);
    }

    @Test
    public void testInvalidServerSideS3EncryptionStrategyBecauseKeyIdOrMaterialSpecified() {
        configureService(STRATEGY_NAME_SSE_S3, "key-id");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideS3EncryptionStrategyBecauseKeyIdOrMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_SSE_S3, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideS3EncryptionStrategyBecauseKeyIdOrMaterialSpecifiedUsingEL() {
        configureService(STRATEGY_NAME_SSE_S3, "${key-id-var}");

        runner.assertNotValid(service);
    }


    // ServerSideKMSEncryptionStrategy

    @Test
    public void testValidServerSideKMSEncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_KMS, "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testValidServerSideKMSEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}");
        configureVariable("key-id-var", "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdNotSpecified() {
        configureService(STRATEGY_NAME_SSE_KMS, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_SSE_KMS, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToNull() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}");
        configureVariable("key-id-var", "");

        runner.assertNotValid(service);
    }


    // ServerSideCEncryptionStrategy

    @Test
    public void testValidServerSideCEncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_C, createKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testValidServerSideCEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_SSE_C, "${key-material-var}");
        configureVariable("key-material-var", createKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialNotSpecified() {
        configureService(STRATEGY_NAME_SSE_C, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_SSE_C, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToNull() {
        configureService(STRATEGY_NAME_SSE_C, "${key-material-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_SSE_C, "${key-material-var}");
        configureVariable("key-material-var", "");

        runner.assertNotValid(service);
    }


    // ClientSideKMSEncryptionStrategy

    @Test
    public void testValidClientSideKMSEncryptionStrategy() {
        configureService(STRATEGY_NAME_CSE_KMS, "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testValidClientSideKMSEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}");
        configureVariable("key-id-var", "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdNotSpecified() {
        configureService(STRATEGY_NAME_CSE_KMS, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_CSE_KMS, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToNull() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}");
        configureVariable("key-id-var", "");

        runner.assertNotValid(service);
    }


    // ClientSideCEncryptionStrategy

    @Test
    public void testValidClientSideCEncryptionStrategy() {
        configureService(STRATEGY_NAME_CSE_C, createKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testValidClientSideCEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_CSE_C, "${key-material-var}");
        configureVariable("key-material-var", createKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialNotSpecified() {
        configureService(STRATEGY_NAME_CSE_C, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_CSE_C, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToNull() {
        configureService(STRATEGY_NAME_CSE_C, "${key-material-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_CSE_C, "${key-material-var}");
        configureVariable("key-material-var", "");

        runner.assertNotValid(service);
    }


    private void configureService(String encryptionStrategy, String keyIdOrMaterial) {
        runner.setProperty(service, StandardS3EncryptionService.ENCRYPTION_STRATEGY, encryptionStrategy);
        if (keyIdOrMaterial != null) {
            runner.setProperty(service, StandardS3EncryptionService.ENCRYPTION_VALUE, keyIdOrMaterial);
        }
    }

    private void configureVariable(String name, String value) {
        runner.setVariable(name, value);
    }
}
