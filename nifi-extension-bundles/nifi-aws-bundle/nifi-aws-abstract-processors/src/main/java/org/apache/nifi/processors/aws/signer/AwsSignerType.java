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
package org.apache.nifi.processors.aws.signer;

import com.amazonaws.auth.SignerFactory;
import org.apache.nifi.components.DescribedValue;

import java.util.HashMap;
import java.util.Map;

public enum AwsSignerType implements DescribedValue {

    // AWS_***_SIGNERs must follow the names in com.amazonaws.auth.SignerFactory and com.amazonaws.services.s3.AmazonS3Client
    DEFAULT_SIGNER("Default Signature", "Default Signature"),
    AWS_V4_SIGNER(SignerFactory.VERSION_FOUR_SIGNER, "Signature Version 4"),
    AWS_S3_V4_SIGNER("AWSS3V4SignerType", "Signature Version 4"), // AmazonS3Client.S3_V4_SIGNER
    AWS_S3_V2_SIGNER("S3SignerType", "Signature Version 2"), // AmazonS3Client.S3_SIGNER
    CUSTOM_SIGNER("CustomSignerType", "Custom Signature");

    private static final Map<String, AwsSignerType> LOOKUP_MAP = new HashMap<>();

    static {
        for (AwsSignerType signerType : values()) {
            LOOKUP_MAP.put(signerType.getValue(), signerType);
        }
    }

    private final String value;
    private final String displayName;
    private final String description;

    AwsSignerType(String value, String displayName) {
        this(value, displayName, null);
    }

    AwsSignerType(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static AwsSignerType forValue(final String value) {
        return LOOKUP_MAP.get(value);
    }
}
