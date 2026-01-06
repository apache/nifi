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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.encryption.s3.CommitmentPolicy;
import software.amazon.encryption.s3.S3EncryptionClient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Tags({"service", "aws", "s3", "encryption", "encrypt", "decryption", "decrypt", "key"})
@CapabilityDescription("Adds configurable encryption to S3 Put and S3 Fetch operations.")
public class StandardS3EncryptionService extends AbstractControllerService implements AmazonS3EncryptionService {
    private static final Logger logger = LoggerFactory.getLogger(StandardS3EncryptionService.class);

    private static final String OBSOLETE_ENCRYPTION_VALUE_1 = "key-id-or-key-material";
    private static final String OBSOLETE_ENCRYPTION_VALUE_2 = "Key ID or Key Material";
    private static final String OBSOLETE_KMS_REGION_1 = "kms-region";
    private static final String OBSOLETE_KMS_REGION_2 = "KMS Region";

    private static final Map<String, S3EncryptionStrategy> NAMED_STRATEGIES = Map.of(
            STRATEGY_NAME_NONE, new NoOpEncryptionStrategy(),
            STRATEGY_NAME_SSE_S3, new ServerSideS3EncryptionStrategy(),
            STRATEGY_NAME_SSE_KMS, new ServerSideKMSEncryptionStrategy(),
            STRATEGY_NAME_SSE_C, new ServerSideCEncryptionStrategy(),
            STRATEGY_NAME_CSE_KMS, new ClientSideKMSEncryptionStrategy(),
            STRATEGY_NAME_CSE_C, new ClientSideCEncryptionStrategy()
    );

    private static final AllowableValue NONE = new AllowableValue(STRATEGY_NAME_NONE, "None", "No encryption.");
    private static final AllowableValue SSE_S3 = new AllowableValue(STRATEGY_NAME_SSE_S3, "Server-side S3", "Use server-side, S3-managed encryption.");
    private static final AllowableValue SSE_KMS = new AllowableValue(STRATEGY_NAME_SSE_KMS, "Server-side KMS", "Use server-side, KMS key to perform encryption.");
    private static final AllowableValue SSE_C = new AllowableValue(STRATEGY_NAME_SSE_C, "Server-side Customer Key", "Use server-side, customer-supplied key to perform encryption.");
    private static final AllowableValue CSE_KMS = new AllowableValue(STRATEGY_NAME_CSE_KMS, "Client-side KMS", "Use client-side, KMS key to perform encryption.");
    private static final AllowableValue CSE_C = new AllowableValue(STRATEGY_NAME_CSE_C, "Client-side Customer Key", "Use client-side, customer-supplied key to perform encryption.");

    public static final Map<String, AllowableValue> ENCRYPTION_STRATEGY_ALLOWABLE_VALUES =
            Map.of(STRATEGY_NAME_NONE, NONE,
                    STRATEGY_NAME_SSE_S3, SSE_S3,
                    STRATEGY_NAME_SSE_KMS, SSE_KMS,
                    STRATEGY_NAME_SSE_C, SSE_C,
                    STRATEGY_NAME_CSE_KMS, CSE_KMS,
                    STRATEGY_NAME_CSE_C, CSE_C);

    public static final PropertyDescriptor ENCRYPTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Encryption Strategy")
            .description("Strategy to use for S3 data encryption and decryption.")
            .allowableValues(NONE, SSE_S3, SSE_KMS, SSE_C, CSE_KMS, CSE_C)
            .required(true)
            .defaultValue(NONE.getValue())
            .build();

    public static final PropertyDescriptor KMS_KEY_ID = new PropertyDescriptor.Builder()
            .name("KMS Key ID")
            .description("The ID (ARN) of the KMS key used for Server-side or Client-side KMS encryption.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(ENCRYPTION_STRATEGY, SSE_KMS, CSE_KMS)
            .build();

    public static final PropertyDescriptor KEY_MATERIAL = new PropertyDescriptor.Builder()
            .name("Key Material")
            .description("The key material used for Server-side or Client-side Customer Key encryption. The Key Material must be specified in Base64 encoded form. " +
                    "In case of Server-side Customer Key, the key must be an AES-256 key. In case of Client-side Customer Key, it can be an AES-256, AES-192 or AES-128 key.")
            .required(true)
            .sensitive(true)
            .addValidator(Validator.VALID) // will be validated in customValidate()
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(ENCRYPTION_STRATEGY, SSE_C, CSE_C)
            .build();

    public static final PropertyDescriptor COMMITMENT_POLICY = new PropertyDescriptor.Builder()
            .name("Commitment Policy")
            .description("The commitment policy for client-side encryption. Key commitment ensures that the data key used for encryption is " +
                    "cryptographically bound to the ciphertext. This prevents certain types of attacks where an attacker could manipulate the ciphertext. " +
                    "When migrating from older encryption (pre-4.0.0 S3 Encryption Client), use 'Require Encrypt Allow Decrypt' to encrypt new data with " +
                    "key-committing algorithms while still being able to decrypt legacy data.")
            .required(true)
            .allowableValues(S3EncryptionCommitmentPolicy.class)
            .defaultValue(S3EncryptionCommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT)
            .dependsOn(ENCRYPTION_STRATEGY, CSE_KMS, CSE_C)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ENCRYPTION_STRATEGY,
            KMS_KEY_ID,
            KEY_MATERIAL,
            COMMITMENT_POLICY
    );

    private S3EncryptionKeySpec keySpec;
    private S3EncryptionStrategy encryptionStrategy = new NoOpEncryptionStrategy();
    private String strategyName = STRATEGY_NAME_NONE;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final String newStrategyName = context.getProperty(ENCRYPTION_STRATEGY).getValue();
        final S3EncryptionStrategy newEncryptionStrategy = NAMED_STRATEGIES.get(newStrategyName);

        if (newEncryptionStrategy == null) {
            final String msg = "No encryption strategy found for name: " + strategyName;
            logger.warn(msg);
            throw new InitializationException(msg);
        }

        strategyName = newStrategyName;
        encryptionStrategy = newEncryptionStrategy;
        keySpec = createKeySpec(context, newStrategyName);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        final String encryptionStrategyName = validationContext.getProperty(ENCRYPTION_STRATEGY).getValue();
        final S3EncryptionStrategy encryptionStrategy = NAMED_STRATEGIES.get(encryptionStrategyName);

        final S3EncryptionKeySpec keySpec = createKeySpec(validationContext, encryptionStrategyName);

        validationResults.add(encryptionStrategy.validateKeySpec(keySpec));

        return validationResults;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("encryption-strategy", ENCRYPTION_STRATEGY.getName());

        migrateEncryptionValue(config);

        config.removeProperty(OBSOLETE_KMS_REGION_1);
        config.removeProperty(OBSOLETE_KMS_REGION_2);
    }

    private void migrateEncryptionValue(PropertyConfiguration config) {
        final String propertyName;
        if (config.hasProperty(OBSOLETE_ENCRYPTION_VALUE_1)) {
            propertyName = OBSOLETE_ENCRYPTION_VALUE_1;
        } else if (config.hasProperty(OBSOLETE_ENCRYPTION_VALUE_2)) {
            propertyName = OBSOLETE_ENCRYPTION_VALUE_2;
        } else {
            propertyName = null;
        }

        if (propertyName != null) {
            config.getPropertyValue(propertyName).ifPresent(encryptionValue -> {
                final String strategyName = config.getPropertyValue(ENCRYPTION_STRATEGY).orElse(STRATEGY_NAME_NONE);

                switch (strategyName) {
                    case STRATEGY_NAME_SSE_KMS, STRATEGY_NAME_CSE_KMS -> config.setProperty(KMS_KEY_ID, encryptionValue);
                    case STRATEGY_NAME_SSE_C, STRATEGY_NAME_CSE_C -> config.setProperty(KEY_MATERIAL, encryptionValue);
                }
            });

            config.removeProperty(propertyName);
        }
    }

    @Override
    public void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder) {
        encryptionStrategy.configurePutObjectRequest(requestBuilder, keySpec);
    }

    @Override
    public void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder) {
        encryptionStrategy.configureCreateMultipartUploadRequest(requestBuilder, keySpec);
    }

    @Override
    public void configureGetObjectRequest(GetObjectRequest.Builder requestBuilder) {
        encryptionStrategy.configureGetObjectRequest(requestBuilder, keySpec);
    }

    @Override
    public void configureUploadPartRequest(UploadPartRequest.Builder requestBuilder) {
        encryptionStrategy.configureUploadPartRequest(requestBuilder, keySpec);
    }

    @Override
    public S3EncryptionClient.Builder createEncryptionClientBuilder() {
        return encryptionStrategy.createEncryptionClientBuilder(keySpec);
    }

    @Override
    public String getStrategyName() {
        return strategyName;
    }

    @Override
    public String getStrategyDisplayName() {
        return ENCRYPTION_STRATEGY_ALLOWABLE_VALUES.get(strategyName).getDisplayName();
    }

    private S3EncryptionKeySpec createKeySpec(final PropertyContext context, final String encryptionStrategyName) {
        final CommitmentPolicy commitmentPolicy = getCommitmentPolicy(context, encryptionStrategyName);

        switch (encryptionStrategyName) {
            case STRATEGY_NAME_NONE:
            case STRATEGY_NAME_SSE_S3:
                return new S3EncryptionKeySpec(null, null, null, null);
            case STRATEGY_NAME_SSE_KMS:
                final String sseKmsKeyId = context.getProperty(KMS_KEY_ID).evaluateAttributeExpressions().getValue();
                return new S3EncryptionKeySpec(sseKmsKeyId, null, null, null);
            case STRATEGY_NAME_CSE_KMS:
                final String cseKmsKeyId = context.getProperty(KMS_KEY_ID).evaluateAttributeExpressions().getValue();
                return new S3EncryptionKeySpec(cseKmsKeyId, null, null, commitmentPolicy);
            case STRATEGY_NAME_SSE_C:
                final String sseKeyMaterial = context.getProperty(KEY_MATERIAL).evaluateAttributeExpressions().getValue();
                final String sseKeyMaterialMd5 = calculateMd5(sseKeyMaterial);
                return new S3EncryptionKeySpec(null, sseKeyMaterial, sseKeyMaterialMd5, null);
            case STRATEGY_NAME_CSE_C:
                final String cseKeyMaterial = context.getProperty(KEY_MATERIAL).evaluateAttributeExpressions().getValue();
                final String cseKeyMaterialMd5 = calculateMd5(cseKeyMaterial);
                return new S3EncryptionKeySpec(null, cseKeyMaterial, cseKeyMaterialMd5, commitmentPolicy);
            default:
                throw new IllegalArgumentException("Unknown encryption strategy: " + encryptionStrategyName);
        }
    }

    private CommitmentPolicy getCommitmentPolicy(final PropertyContext context, final String encryptionStrategyName) {
        if (STRATEGY_NAME_CSE_KMS.equals(encryptionStrategyName) || STRATEGY_NAME_CSE_C.equals(encryptionStrategyName)) {
            final S3EncryptionCommitmentPolicy policy = context.getProperty(COMMITMENT_POLICY).asAllowableValue(S3EncryptionCommitmentPolicy.class);
            return policy.getCommitmentPolicy();
        }
        return null;
    }

    private String calculateMd5(final String keyMaterial) {
        try {
            return Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("MD5").digest(
                            Base64.getDecoder().decode(keyMaterial)
                    )
            );
        } catch (NoSuchAlgorithmException e) {
            throw new ProcessException("Failed to calculate MD5 hash for Key Material", e);
        }
    }
}


