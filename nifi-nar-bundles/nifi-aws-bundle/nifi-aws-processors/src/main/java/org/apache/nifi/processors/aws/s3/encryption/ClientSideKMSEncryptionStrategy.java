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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import org.apache.commons.lang3.StringUtils;

/**
 * This strategy uses KMS key id to perform client-side encryption.  Use this strategy when you want the client to perform the encryption,
 * (thus incurring the cost of processing) and manage the key in a KMS instance.
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html#client-side-encryption-kms-managed-master-key-intro
 *
 */
public class ClientSideKMSEncryptionStrategy implements S3EncryptionStrategy {
    /**
     * Create an encryption client.
     *
     * @param credentialsProvider AWS credentials provider.
     * @param clientConfiguration Client configuration
     * @param kmsRegion AWS KMS region
     * @param keyIdOrMaterial KMS key id
     * @return AWS S3 client
     */
    @Override
    public AmazonS3Client createEncryptionClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, String kmsRegion, String keyIdOrMaterial) {
        KMSEncryptionMaterialsProvider materialProvider = new KMSEncryptionMaterialsProvider(keyIdOrMaterial);
        boolean haveKmsRegion = StringUtils.isNotBlank(kmsRegion);

        CryptoConfiguration cryptoConfig = new CryptoConfiguration();
        if (haveKmsRegion) {
            Region awsRegion = Region.getRegion(Regions.fromName(kmsRegion));
            cryptoConfig.setAwsKmsRegion(awsRegion);
        }

        AmazonS3EncryptionClient client = new AmazonS3EncryptionClient(credentialsProvider, materialProvider, cryptoConfig);

        return client;
    }
}
