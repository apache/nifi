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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_CSE_C;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_CSE_KMS;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_NONE;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_C;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS;
import static org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3;
import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createCustomerKey;

public class TestStandardS3EncryptionServiceValidation {

    private TestRunner runner;
    private StandardS3EncryptionService service;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardS3EncryptionService();
        runner.addControllerService("s3-encryption-service", service);
    }


    // NoOpEncryptionStrategy

    @Test
    public void testValidNoOpEncryptionStrategy() {
        configureService(STRATEGY_NAME_NONE, null, null);

        runner.assertValid(service);
    }


    // ServerSideS3EncryptionStrategy

    @Test
    public void testValidServerSideS3EncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_S3, null, null);

        runner.assertValid(service);
    }


    // ServerSideKMSEncryptionStrategy

    @Test
    public void testValidServerSideKMSEncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_KMS, "key-id", null);

        runner.assertValid(service);
    }

    @Test
    public void testValidServerSideKMSEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}", null);
        configureVariable("key-id-var", "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdNotSpecified() {
        configureService(STRATEGY_NAME_SSE_KMS, null, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_SSE_KMS, "", null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToNull() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}", null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_SSE_KMS, "${key-id-var}", null);
        configureVariable("key-id-var", "");

        runner.assertNotValid(service);
    }


    // ServerSideCEncryptionStrategy

    @Test
    public void testValidServerSideCEncryptionStrategy() {
        configureService(STRATEGY_NAME_SSE_C, null, createCustomerKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testValidServerSideCEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_SSE_C, null, "${key-material-var}");
        configureVariable("key-material-var", createCustomerKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialNotSpecified() {
        configureService(STRATEGY_NAME_SSE_C, null, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_SSE_C, null, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToNull() {
        configureService(STRATEGY_NAME_SSE_C, null, "${key-material-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidServerSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_SSE_C, null, "${key-material-var}");
        configureVariable("key-material-var", "");

        runner.assertNotValid(service);
    }


    // ClientSideKMSEncryptionStrategy

    @Test
    public void testValidClientSideKMSEncryptionStrategy() {
        configureService(STRATEGY_NAME_CSE_KMS, "key-id", null);

        runner.assertValid(service);
    }

    @Test
    public void testValidClientSideKMSEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}", null);
        configureVariable("key-id-var", "key-id");

        runner.assertValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdNotSpecified() {
        configureService(STRATEGY_NAME_CSE_KMS, null, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_CSE_KMS, "", null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToNull() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}", null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideKMSEncryptionStrategyBecauseKeyIdEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_CSE_KMS, "${key-id-var}", null);
        configureVariable("key-id-var", "");

        runner.assertNotValid(service);
    }


    // ClientSideCEncryptionStrategy

    @Test
    public void testValidClientSideCEncryptionStrategy() {
        configureService(STRATEGY_NAME_CSE_C, null, createCustomerKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testValidClientSideCEncryptionStrategyUsingEL() {
        configureService(STRATEGY_NAME_CSE_C, null, "${key-material-var}");
        configureVariable("key-material-var", createCustomerKey(256));

        runner.assertValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialNotSpecified() {
        configureService(STRATEGY_NAME_CSE_C, null, null);

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialSpecifiedAsEmptyString() {
        configureService(STRATEGY_NAME_CSE_C, null, "");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToNull() {
        configureService(STRATEGY_NAME_CSE_C, null, "${key-material-var}");

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidClientSideCEncryptionStrategyBecauseKeyMaterialEvaluatedToEmptyString() {
        configureService(STRATEGY_NAME_CSE_C, null, "${key-material-var}");
        configureVariable("key-material-var", "");

        runner.assertNotValid(service);
    }


    private void configureService(String encryptionStrategy, String keyId, String keyMaterial) {
        runner.setProperty(service, StandardS3EncryptionService.ENCRYPTION_STRATEGY, encryptionStrategy);
        if (keyId != null) {
            runner.setProperty(service, StandardS3EncryptionService.KMS_KEY_ID, keyId);
        }
        if (keyMaterial != null) {
            runner.setProperty(service, StandardS3EncryptionService.KEY_MATERIAL, keyMaterial);
        }
    }

    private void configureVariable(String name, String value) {
        runner.setEnvironmentVariableValue(name, value);
    }
}
