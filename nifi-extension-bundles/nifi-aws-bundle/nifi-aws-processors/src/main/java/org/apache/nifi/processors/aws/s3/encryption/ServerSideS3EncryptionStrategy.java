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

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * This strategy uses S3-managed keys to perform server-side encryption.  Use this strategy when you want the server to
 * perform the encryption (meaning you pay the cost of processing) and you want AWS to completely manage the key.
 *
 *
 * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">Using Server Side Encryption</a>
 *
 */
public class ServerSideS3EncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
    }

    @Override
    public void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
    }
}
