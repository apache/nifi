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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientV2;
import com.amazonaws.services.s3.AmazonS3EncryptionClientV2Builder;
import com.amazonaws.services.s3.model.CryptoConfigurationV2;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Consumer;

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
     * @param clientBuilder A consumer that is responsible for configuring the client builder
     * @param kmsRegion AWS KMS region
     * @param keyIdOrMaterial KMS key id
     * @return AWS S3 client
     */
    @Override
    public AmazonS3 createEncryptionClient(final Consumer<AmazonS3Builder<?, ?>> clientBuilder, final String kmsRegion, final String keyIdOrMaterial) {
        final KMSEncryptionMaterialsProvider encryptionMaterialsProvider = new KMSEncryptionMaterialsProvider(keyIdOrMaterial);

        final CryptoConfigurationV2 cryptoConfig = new CryptoConfigurationV2();
        if (StringUtils.isNotBlank(kmsRegion)) {
            cryptoConfig.setAwsKmsRegion(Region.getRegion(Regions.fromName(kmsRegion)));
        }

        final AmazonS3EncryptionClientV2Builder builder = AmazonS3EncryptionClientV2.encryptionBuilder()
                .withCryptoConfiguration(cryptoConfig)
                .withEncryptionMaterialsProvider(encryptionMaterialsProvider);
        clientBuilder.accept(builder);
        return builder.build();
    }
}
