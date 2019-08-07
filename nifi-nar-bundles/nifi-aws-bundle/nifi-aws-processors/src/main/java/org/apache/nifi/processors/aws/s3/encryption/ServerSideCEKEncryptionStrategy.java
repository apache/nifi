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

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.nifi.components.ValidationResult;
import org.bouncycastle.util.encoders.Base64;

/**
 * This strategy uses a customer key to perform server-side encryption.  Use this strategy when you want the server to perform the encryption,
 * (meaning you pay cost of processing) and when you want to manage the key material yourself.
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
 *
 */
public class ServerSideCEKEncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public void configurePutObjectRequest(PutObjectRequest request, ObjectMetadata objectMetadata, String keyValue) {
        SSECustomerKey customerKey = new SSECustomerKey(keyValue);
        request.setSSECustomerKey(customerKey);
    }

    @Override
    public void configureInitiateMultipartUploadRequest(InitiateMultipartUploadRequest request, ObjectMetadata objectMetadata, String keyValue) {
        SSECustomerKey customerKey = new SSECustomerKey(keyValue);
        request.setSSECustomerKey(customerKey);
    }

    @Override
    public void configureGetObjectRequest(GetObjectRequest request, ObjectMetadata objectMetadata, String keyValue) {
        SSECustomerKey customerKey = new SSECustomerKey(keyValue);
        request.setSSECustomerKey(customerKey);
    }

    @Override
    public void configureUploadPartRequest(UploadPartRequest request, ObjectMetadata objectMetadata, String keyValue) {
        SSECustomerKey customerKey = new SSECustomerKey(keyValue);
        request.setSSECustomerKey(customerKey);
    }

    @Override
    public ValidationResult validateKey(String keyValue) {
        boolean decoded = false;
        boolean sized = false;
        byte[] keyMaterial;

        try {
            keyMaterial = Base64.decode(keyValue);
            decoded = true;
            sized = (keyMaterial.length > 0) && (keyMaterial.length % 32) == 0;
        } catch (final Exception ignored) {
        }

        return new ValidationResult.Builder().valid(decoded && sized).build();
    }
}
