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
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import org.bouncycastle.util.encoders.Base64;

import javax.crypto.spec.SecretKeySpec;

/**
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html#client-side-encryption-client-side-master-key-intro
 *
 *
 */
public class ClientSideCMKEncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public AmazonS3Client createEncryptionClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, String region, String keyIdOrMaterial) {
        byte[] keyMaterial = Base64.decode(keyIdOrMaterial);
        SecretKeySpec symmetricKey = new SecretKeySpec(keyMaterial, "AES");
        StaticEncryptionMaterialsProvider encryptionMaterialsProvider = new StaticEncryptionMaterialsProvider(new EncryptionMaterials(symmetricKey));
        boolean haveRegion = StringUtils.isNotBlank(region);
        CryptoConfiguration cryptoConfig = new CryptoConfiguration();

        if (haveRegion) {
            cryptoConfig.setAwsKmsRegion(Region.getRegion(Regions.fromName(region)));
        }

        AmazonS3EncryptionClient client = new AmazonS3EncryptionClient(credentialsProvider, encryptionMaterialsProvider, cryptoConfig);
        if (haveRegion) {
            client.setRegion(Region.getRegion(Regions.fromName(region)));
        }

        return client;
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
