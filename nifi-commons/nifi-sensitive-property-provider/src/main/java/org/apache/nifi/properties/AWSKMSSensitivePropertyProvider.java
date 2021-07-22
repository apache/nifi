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
package org.apache.nifi.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsException;

import java.util.Base64;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;

public class AWSKMSSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AWSKMSSensitivePropertyProvider.class);

    private static final String AWS_PREFIX = "aws";
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";
    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";
    private static final String REGION_KEY_PROPS_NAME = "aws.region";
    private static final String KMS_KEY_PROPS_NAME = "aws.kms.key.id";

    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;

    private final BootstrapProperties awsBootstrapProperties;
    private KmsClient client;
    private String keyId;


    AWSKMSSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) throws SensitivePropertyProtectionException {
        super(bootstrapProperties);
        Objects.requireNonNull(bootstrapProperties, "The file bootstrap.conf provided to AWS SPP is null");
        awsBootstrapProperties = getAWSBootstrapProperties(bootstrapProperties);
        loadRequiredAWSProperties(awsBootstrapProperties);
    }

    /**
     * Initializes the KMS Client to be used for encrypt, decrypt and other interactions with AWS KMS.
     * First attempts to use credentials/configuration in bootstrap-aws.conf.
     * If credentials/configuration in bootstrap-aws.conf is not fully configured,
     * attempt to initialize credentials using default AWS credentials/configuration chain.
     * Note: This does not verify if credentials are valid.
     */
    private void initializeClient() {
        if (awsBootstrapProperties == null) {
            logger.warn("AWS Bootstrap Properties are required for KMS Client initialization");
            return;
        }
        final String accessKey = awsBootstrapProperties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKey = awsBootstrapProperties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = awsBootstrapProperties.getProperty(REGION_KEY_PROPS_NAME);

        if (StringUtils.isNoneBlank(accessKey, secretKey, region)) {
            logger.debug("Using AWS credentials from bootstrap properties");
            try {
                final AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                client = KmsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .build();
            } catch (final RuntimeException e) {
                final String msg = "Valid configuration/credentials are required to initialize KMS client";
                throw new SensitivePropertyProtectionException(msg, e);
            }
        } else {
            logger.debug("Using AWS credentials from default credentials provider");
            try {
                final DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder()
                        .build();
                credentialsProvider.resolveCredentials();
                client = KmsClient.builder()
                        .credentialsProvider(credentialsProvider)
                        .build();
            } catch (final SdkClientException e) {
                final String msg = "Valid configuration/credentials are required to initialize KMS client";
                throw new SensitivePropertyProtectionException(msg, e);
            }
        }
    }

    /**
     * Validates the key ARN, credentials and configuration provided by the user.
     * Note: This function performs checks on the key and indirectly also validates the credentials and
     * configurations provided during the initialization of the client.
     */
    private void validate() throws KmsException, SensitivePropertyProtectionException {
        if (client == null) {
            final String msg = "The AWS KMS Client failed to open, cannot validate key";
            throw new SensitivePropertyProtectionException(msg);
        }
        if (StringUtils.isBlank(keyId)) {
            final String msg = "The AWS KMS key provided is blank";
            throw new SensitivePropertyProtectionException(msg);
        }

        // asking for a Key Description is the best way to check whether a key is valid
        // because AWS KMS accepts various formats for its keys.
        final DescribeKeyRequest request = DescribeKeyRequest.builder()
                .keyId(keyId)
                .build();

        // using the KmsClient in a DescribeKey request indirectly also verifies if the credentials provided
        // during the initialization of the key are valid
        final DescribeKeyResponse response = client.describeKey(request);
        final KeyMetadata metadata = response.keyMetadata();

        if (!metadata.enabled()) {
            final String msg = String.format("AWS KMS key [%s] is not enabled", keyId);
            throw new SensitivePropertyProtectionException(msg);
        }
    }

    /**
     * Checks if we have a key ID from AWS KMS and loads it into {@link #keyId}. Will load null if key is not present.
     * Note: This function does not verify if the key is correctly formatted/valid.
     * @param props the properties representing bootstrap-aws.conf
     */
    private void loadRequiredAWSProperties(final BootstrapProperties props) {
        if (props != null) {
            keyId = props.getProperty(KMS_KEY_PROPS_NAME);
        }
    }


    /**
     * Checks bootstrap.conf to check if BootstrapPropertyKey.AWS_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF property is
     * configured to the bootstrap-aws.conf file. Also will try to load bootstrap-aws.conf to {@link #awsBootstrapProperties}.
     * @param bootstrapProperties BootstrapProperties object corresponding to bootstrap.conf.
     * @return BootstrapProperties object corresponding to bootstrap-aws.conf, null otherwise.
     */
    private BootstrapProperties getAWSBootstrapProperties(final BootstrapProperties bootstrapProperties) {
        final BootstrapProperties cloudBootstrapProperties;

        // Load the bootstrap-aws.conf file based on path specified in
        // "nifi.bootstrap.protection.aws.kms.conf" property of bootstrap.conf
        final String filePath = bootstrapProperties.getProperty(BootstrapPropertyKey.AWS_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF).orElse(null);
        if (StringUtils.isBlank(filePath)) {
            logger.warn("AWS KMS properties file path not configured in bootstrap properties");
            return null;
        }

        try {
            cloudBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                    Paths.get(filePath), AWS_PREFIX);
        } catch (final IOException e) {
            throw new SensitivePropertyProtectionException("Could not load " + filePath, e);
        }

        return cloudBootstrapProperties;
    }

    /**
     * Checks bootstrap-aws.conf for the required configurations for AWS KMS encrypt/decrypt operations.
     * Note: This does not check for credentials/region configurations.
     *       Credentials/configuration will be checked during the first protect/unprotect call during runtime.
     * @return true if bootstrap-aws.conf contains the required properties for AWS SPP, false otherwise.
     */
    private boolean hasRequiredAWSProperties() {
        return awsBootstrapProperties != null && StringUtils.isNotBlank(keyId);
    }

    @Override
    public boolean isSupported() {
        return hasRequiredAWSProperties();
    }

    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.AWS_KMS;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider.
     */
    @Override
    public String getName() {
        return PropertyProtectionScheme.AWS_KMS.getName();
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property.
     */
    @Override
    public String getIdentifierKey() {
        return PropertyProtectionScheme.AWS_KMS.getIdentifier();
    }


    /**
     * Returns the ciphertext blob of this value encrypted using an AWS KMS CMK.
     *
     * @return the ciphertext blob to persist in the {@code nifi.properties} file.
     */
    private byte[] encrypt(final byte[] input) {
        final SdkBytes plainBytes = SdkBytes.fromByteArray(input);

        final EncryptRequest encryptRequest = EncryptRequest.builder()
                .keyId(keyId)
                .plaintext(plainBytes)
                .build();

        final EncryptResponse response = client.encrypt(encryptRequest);
        final SdkBytes encryptedData = response.ciphertextBlob();

        return encryptedData.asByteArray();
    }

    /**
     * Returns the value corresponding to a ciphertext blob decrypted using an AWS KMS CMK.
     *
     * @return the "unprotected" byte[] of this value, which could be used by the application.
     */
    private byte[] decrypt(final byte[] input) {
        final SdkBytes cipherBytes = SdkBytes.fromByteArray(input);

        final DecryptRequest decryptRequest = DecryptRequest.builder()
                .ciphertextBlob(cipherBytes)
                .keyId(keyId)
                .build();

        final DecryptResponse response = client.decrypt(decryptRequest);
        final SdkBytes decryptedData = response.plaintext();

        return decryptedData.asByteArray();
    }

    /**
     * Checks if the client is open and if not, initializes the client and validates the key required for AWS KMS.
     */
    private void checkAndInitializeClient() throws SensitivePropertyProtectionException {
        if (client == null) {
            try {
                initializeClient();
                validate();
            } catch (final SdkClientException | KmsException | SensitivePropertyProtectionException e) {
                throw new SensitivePropertyProtectionException("Error initializing the AWS KMS Client", e);
            }
        }
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * Encrypts a sensitive value using a key managed by AWS Key Management Service.
     *
     * @param unprotectedValue the sensitive value.
     * @param context The context of the value (ignored in this implementation)
     * @return the value to persist in the {@code nifi.properties} file.
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            final byte[] plainBytes = unprotectedValue.getBytes(PROPERTY_CHARSET);
            final byte[] cipherBytes = encrypt(plainBytes);
            return Base64.getEncoder().encodeToString(cipherBytes);
        } catch (final SdkClientException | KmsException e) {
            throw new SensitivePropertyProtectionException("Encrypt failed", e);
        }
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * Decrypts a secured value from a ciphertext using a key managed by AWS Key Management Service.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file.
     * @param context The context of the value (ignored in this implementation)
     * @return the raw value to be used by the application.
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(protectedValue)) {
            throw new IllegalArgumentException("Cannot decrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            final byte[] cipherBytes = Base64.getDecoder().decode(protectedValue);
            final byte[] plainBytes = decrypt(cipherBytes);
            return new String(plainBytes, PROPERTY_CHARSET);
        } catch (final SdkClientException | KmsException e) {
            throw new SensitivePropertyProtectionException("Decrypt failed", e);
        }
    }

    /**
     * Closes AWS KMS client that may have been opened.
     */
    @Override
    public void cleanUp() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
