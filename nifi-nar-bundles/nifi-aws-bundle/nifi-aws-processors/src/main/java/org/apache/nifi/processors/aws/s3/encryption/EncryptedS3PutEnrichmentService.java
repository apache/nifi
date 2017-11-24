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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.s3.service.S3PutEnrichmentService;
import org.apache.nifi.reporting.InitializationException;

@Tags({"aws", "s3", "encryption", "server", "kms", "key"})
@CapabilityDescription("Provides the ability to configure S3 Server Side Encryption once and reuse " +
        "that configuration throughout the application")
public class EncryptedS3PutEnrichmentService extends AbstractControllerService implements S3PutEnrichmentService {

    public static final String METHOD_SSE_S3 = "SSE-S3";
    public static final String METHOD_SSE_KMS = "SSE-KMS";
    public static final String METHOD_SSE_C = "SSE-C";

    public static final String ALGORITHM_AES256 = "AES256";
    public static final String CUSTOMER_ALGORITHM_AES256 = "AES256";


    public static final PropertyDescriptor ENCRYPTION_METHOD = new PropertyDescriptor.Builder()
            .name("encryption-method")
            .displayName("Encryption Method")
            .required(true)
            .allowableValues(METHOD_SSE_S3, METHOD_SSE_KMS, METHOD_SSE_C)
            .defaultValue(METHOD_SSE_S3)
            .description("Method by which the S3 object will be encrypted server-side.")
            .build();

    public static final PropertyDescriptor ALGORITHM = new PropertyDescriptor.Builder()
            .name("sse-algorithm")
            .displayName("Algorithm")
            .allowableValues(ALGORITHM_AES256)
            .defaultValue(ALGORITHM_AES256)
            .description("Encryption algorithm to use (only AES256 currently supported)")
            .build();

    public static final PropertyDescriptor KMS_KEY_ID = new PropertyDescriptor.Builder()
            .name("sse-kme-key-id")
            .displayName("KMS Key Id")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Custom KMS key identifier. Supports key or alias ARN.")
            .build();

    public static final PropertyDescriptor CUSTOMER_KEY = new PropertyDescriptor.Builder()
            .name("sse-customer-key")
            .displayName("Customer Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Customer provided 256-bit, base64-encoded encryption key.")
            .build();

    public static final PropertyDescriptor CUSTOMER_ALGORITHM = new PropertyDescriptor.Builder()
            .name("sse-customer-algorithm")
            .displayName("Customer Algorithm")
            .allowableValues(CUSTOMER_ALGORITHM_AES256)
            .defaultValue(CUSTOMER_ALGORITHM_AES256)
            .description("Customer encryption algorithm to use (only AES256 currently supported)")
            .build();

    private static final List<PropertyDescriptor> properties;
    private String encryptionMethod;
    private String algorithm;
    private String kmsKeyId;
    private String customerKey;
    private String customerAlgorithm;


    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENCRYPTION_METHOD);
        props.add(ALGORITHM);
        props.add(KMS_KEY_ID);
        props.add(CUSTOMER_KEY);
        props.add(CUSTOMER_ALGORITHM);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        encryptionMethod = context.getProperty(ENCRYPTION_METHOD).getValue();
        algorithm = context.getProperty(ALGORITHM).getValue();
        kmsKeyId = context.getProperty(KMS_KEY_ID).getValue();
        customerKey = context.getProperty(CUSTOMER_KEY).getValue();
        customerAlgorithm = context.getProperty(CUSTOMER_ALGORITHM).getValue();

        if (algorithm == null) algorithm = ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
    }

    public void enrich(PutObjectRequest putObjectRequest) {
        if (encryptionMethod == null) return;

        if (encryptionMethod.equals(METHOD_SSE_S3)) {
            getLogger().info("Encrypting single part object using SSE-S3");
            putObjectRequest.getMetadata().setSSEAlgorithm(algorithm == null ? ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION : algorithm);
        }

        if (encryptionMethod.equals(METHOD_SSE_KMS)) {
            getLogger().info("Encrypting single part object using SSE-KMS");
            putObjectRequest.setSSEAwsKeyManagementParams(kmsKeyId == null ? new SSEAwsKeyManagementParams() : new SSEAwsKeyManagementParams(kmsKeyId));
        }

        if (encryptionMethod.equals(METHOD_SSE_C)) {
            getLogger().info("Encrypting single part object using SSE-C");
            if (StringUtils.isNotBlank(customerKey)) {
                putObjectRequest.setSSECustomerKey(new SSECustomerKey(customerKey));
            }

            String sseCustomerAlgorithm = customerAlgorithm == null ? ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION : customerAlgorithm;
            putObjectRequest.getMetadata().setSSECustomerAlgorithm(sseCustomerAlgorithm);
        }
    }

    public void enrich(InitiateMultipartUploadRequest initiateMultipartUploadRequest) {
        if (encryptionMethod == null) return;

        if (encryptionMethod.equals(METHOD_SSE_S3)) {
            getLogger().info("Encrypting multipart object using SSE-S3");
            initiateMultipartUploadRequest.getObjectMetadata().setSSEAlgorithm(algorithm == null ? ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION : algorithm);
        }

        if (encryptionMethod.equals(METHOD_SSE_KMS)) {
            getLogger().info("Encrypting multipart object using SSE-KMS");
            initiateMultipartUploadRequest.setSSEAwsKeyManagementParams(kmsKeyId == null ? new SSEAwsKeyManagementParams() : new SSEAwsKeyManagementParams(kmsKeyId));
        }

        if (encryptionMethod.equals(METHOD_SSE_C)) {
            getLogger().info("Encrypting multipart object using SSE-C");
            if (StringUtils.isNotBlank(customerKey)) {
                initiateMultipartUploadRequest.setSSECustomerKey(new SSECustomerKey(customerKey));
            }

            String sseCustomerAlgorithm = customerAlgorithm == null ? ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION : customerAlgorithm;
            initiateMultipartUploadRequest.getObjectMetadata().setSSECustomerAlgorithm(sseCustomerAlgorithm);
        }
    }

    @Override
    public String toString() {
        return "EncryptedS3PutEnrichmentService[id=" + getIdentifier() + "]";
    }
}