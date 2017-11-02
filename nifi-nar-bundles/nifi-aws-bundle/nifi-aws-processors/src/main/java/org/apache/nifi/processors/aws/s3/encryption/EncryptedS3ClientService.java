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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.CryptoStorageMode;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.KMSEncryptionMaterials;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.s3.S3ClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

import javax.crypto.spec.SecretKeySpec;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"aws", "s3", "encryption", "client", "kms", "key"})
@CapabilityDescription("Provides the ability to configure S3 Client Side Encryption once and reuse " +
        "that configuration throughout the application")
public class EncryptedS3ClientService extends AbstractControllerService implements S3ClientService {

    public static final String METHOD_CSE_MK = "Client Side Master Key";
    public static final String METHOD_CSE_KMS = "KMS-Managed Customer Master Key";

    public static final PropertyDescriptor ENCRYPTION_METHOD = new PropertyDescriptor.Builder()
            .name("encryption-method")
            .displayName("Encryption Method")
            .required(true)
            .allowableValues(METHOD_CSE_MK, METHOD_CSE_KMS)
            .defaultValue(METHOD_CSE_MK)
            .description("Method by which the S3 object will be encrypted client-side.")
            .build();

    public static final PropertyDescriptor KMS_CMK_ID = new PropertyDescriptor.Builder()
            .name("kms-cmk-id")
            .displayName("KMS Customer Master Key Id")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Identifier belonging to the custom master key managed by the KMS.")
            .build();

    public static final PropertyDescriptor KMS_REGION = new PropertyDescriptor.Builder()
            .name("kms-region")
            .displayName("KMS Region")
            .required(false)
            .allowableValues(getAvailableRegions())
            .description("AWS region that contains the KMS.")
            .build();

    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
            .name("secret-key")
            .displayName("Secret Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Secret key used when performing symmetric client side encryption.")
            .build();

    public static final PropertyDescriptor SECRET_KEY_ALGORITHM = new PropertyDescriptor.Builder()
            .name("secret-key-algorithm")
            .displayName("Secret Key Algorithm")
            .required(false)
            .description("Secret key algorithm used when performing symmetric client side encryption.")
            .build();

    public static final PropertyDescriptor PRIVATE_KEY = new PropertyDescriptor.Builder()
            .name("private-key")
            .displayName("Private Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Private key used when performing asymmetric client side encryption.")
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_ALGORITHM = new PropertyDescriptor.Builder()
            .name("private-key-algorithm")
            .displayName("Private Key Algorithm")
            .required(false)
            .description("Private key algorithm used when performing asymmetric client side encryption.")
            .build();

    public static final PropertyDescriptor PUBLIC_KEY = new PropertyDescriptor.Builder()
            .name("public-key")
            .displayName("Public Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Public key used when performing asymmetric client side encryption.")
            .build();

    public static final PropertyDescriptor PUBLIC_KEY_ALGORITHM = new PropertyDescriptor.Builder()
            .name("public-key-algorithm")
            .displayName("Public Key Algorithm")
            .required(false)
            .description("Public key algorithm used when performing asymmetric client side encryption.")
            .build();

    public static final PropertyDescriptor CRYPTO_MODE = new PropertyDescriptor.Builder()
            .name("crypto-mode")
            .displayName("Crypto Mode")
            .required(false)
            .allowableValues(
                    new AllowableValue(CryptoMode.StrictAuthenticatedEncryption.toString(), "Strict Authenticated Encryption"),
                    new AllowableValue(CryptoMode.AuthenticatedEncryption.toString(), "Authenticated Encryption"),
                    new AllowableValue(CryptoMode.EncryptionOnly.toString(), "Encryption Only"))
            .description("Cryptographic mode used to secure an S3 object.")
            .build();

    public static final PropertyDescriptor CRYPTO_STORAGE_MODE = new PropertyDescriptor.Builder()
            .name("storage-mode")
            .displayName("Crypto Storage Mode")
            .required(false)
            .allowableValues(
                    new AllowableValue(CryptoStorageMode.InstructionFile.toString(), "Instruction File"),
                    new AllowableValue(CryptoStorageMode.ObjectMetadata.toString(), "Object Metadata"))
            .description("Storage mode used to store encryption information when encrypting an S3 object.")
            .build();

    public static final PropertyDescriptor IGNORE_MISSING_INSTRUCTION_FILE = new PropertyDescriptor.Builder()
            .name("ignore-missing-instruction-file")
            .displayName("Ignore Missing Instruction File")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Whether to ignore missing instruction files during decryption.")
            .build();

    private static final List<PropertyDescriptor> properties;
    private String encryptionMethod;
    private String kmsCmkId;
    private String kmsRegion;
    private String secretKey;
    private String secretKeyAlgorithm;
    private String privateKey;
    private String privateKeyAlgorithm;
    private String publicKey;
    private String publicKeyAlgorithm;
    private String cryptoMode;
    private String cryptoStorageMode;
    private Boolean ignoreMissingInstructionFile;


    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENCRYPTION_METHOD);
        props.add(KMS_CMK_ID);
        props.add(KMS_REGION);
        props.add(SECRET_KEY);
        props.add(SECRET_KEY_ALGORITHM);
        props.add(PRIVATE_KEY);
        props.add(PRIVATE_KEY_ALGORITHM);
        props.add(PUBLIC_KEY);
        props.add(PUBLIC_KEY_ALGORITHM);
        props.add(CRYPTO_MODE);
        props.add(CRYPTO_STORAGE_MODE);
        props.add(IGNORE_MISSING_INSTRUCTION_FILE);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        encryptionMethod = context.getProperty(ENCRYPTION_METHOD).getValue();
        kmsCmkId = context.getProperty(KMS_CMK_ID).getValue();
        kmsRegion = context.getProperty(KMS_REGION).getValue();
        secretKey = context.getProperty(SECRET_KEY).getValue();
        secretKeyAlgorithm = context.getProperty(SECRET_KEY_ALGORITHM).getValue();
        privateKey = context.getProperty(PRIVATE_KEY).getValue();
        privateKeyAlgorithm = context.getProperty(PRIVATE_KEY_ALGORITHM).getValue();
        publicKey = context.getProperty(PUBLIC_KEY).getValue();
        publicKeyAlgorithm = context.getProperty(PUBLIC_KEY_ALGORITHM).getValue();
        cryptoMode = context.getProperty(CRYPTO_MODE).getValue();
        cryptoStorageMode = context.getProperty(CRYPTO_STORAGE_MODE).getValue();
        ignoreMissingInstructionFile = context.getProperty(IGNORE_MISSING_INSTRUCTION_FILE).asBoolean();
    }

    private boolean needsEncryptedClient() {
        return encryptionMethod != null && (encryptionMethod.equals(METHOD_CSE_KMS) || encryptionMethod.equals(METHOD_CSE_MK));
    }

    public AmazonS3Client getClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        if (needsEncryptedClient()) {
            return new AmazonS3EncryptionClient(credentialsProvider, new StaticEncryptionMaterialsProvider(encryptionMaterials()), config, cryptoConfiguration());
        } else {
            return new AmazonS3Client(credentialsProvider, config);
        }
    }

    public AmazonS3Client getClient(AWSCredentials credentials, ClientConfiguration config) {
        if (needsEncryptedClient()) {
            return new AmazonS3EncryptionClient(credentials, encryptionMaterials(), config, cryptoConfiguration());
        } else {
            return new AmazonS3Client(credentials, config);
        }
    }

    private CryptoConfiguration cryptoConfiguration() {
        CryptoConfiguration config = new CryptoConfiguration();

        if (!StringUtils.isBlank(cryptoMode)) {
            config.setCryptoMode(CryptoMode.valueOf(cryptoMode));
        }

        if (!StringUtils.isBlank(cryptoStorageMode)) {
            config.setStorageMode(CryptoStorageMode.valueOf(cryptoStorageMode));
        }

        if (!StringUtils.isBlank(kmsRegion)) {
            config.setAwsKmsRegion(Region.getRegion(Regions.fromName(kmsRegion)));
        }

        config.setIgnoreMissingInstructionFile(ignoreMissingInstructionFile);
        return config;
    }

    private EncryptionMaterials encryptionMaterials() {
        if (!StringUtils.isBlank(kmsCmkId)) {
            return new KMSEncryptionMaterials(kmsCmkId);
        }

        if (!StringUtils.isBlank(secretKey)) {
            return new EncryptionMaterials(new SecretKeySpec(secretKey.getBytes(), secretKeyAlgorithm != null ? publicKeyAlgorithm : "RSA"));
        }

        if (!StringUtils.isBlank(publicKey) && !StringUtils.isBlank(privateKey)) {
            try {
                KeyFactory publicKeyFactory = KeyFactory.getInstance(publicKeyAlgorithm != null ? publicKeyAlgorithm : "RSA");
                KeyFactory privateKeyFactory = KeyFactory.getInstance(privateKeyAlgorithm != null ? privateKeyAlgorithm : "RSA");
                X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKey.getBytes());
                PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKey.getBytes());
                KeyPair keyPair =  new KeyPair(publicKeyFactory.generatePublic(publicKeySpec), privateKeyFactory.generatePrivate(privateKeySpec));
                return new EncryptionMaterials(keyPair);
            }catch(Exception e) {
                getLogger().info("Failed to create key pair based encryption materials: reason={}", new Object[]{e.getMessage()});
                return null;
            }
        }

        return null;
    }

    private static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Regions regions : Regions.values()) {
            values.add(new AllowableValue(regions.getName(), regions.getName(), regions.getName()));
        }
        return values.toArray(new AllowableValue[values.size()]);
    }

    @Override
    public String toString() {
        return "EncryptedS3ClientService[id=" + getIdentifier() + "]";
    }
}