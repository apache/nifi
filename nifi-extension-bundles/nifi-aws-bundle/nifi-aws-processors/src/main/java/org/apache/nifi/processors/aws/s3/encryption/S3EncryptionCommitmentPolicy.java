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

import org.apache.nifi.components.DescribedValue;
import software.amazon.encryption.s3.CommitmentPolicy;

/**
 * Enumeration of supported S3 Encryption Commitment Policies
 */
public enum S3EncryptionCommitmentPolicy implements DescribedValue {
    REQUIRE_ENCRYPT_REQUIRE_DECRYPT(
            "Require Encrypt Require Decrypt",
            "Requires key-committing algorithm suites for both encryption and decryption. " +
                    "This is the most secure option but will not decrypt data encrypted with older non-key-committing algorithms.",
            CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    ),
    REQUIRE_ENCRYPT_ALLOW_DECRYPT(
            "Require Encrypt Allow Decrypt",
            "Requires key-committing algorithm suites for encryption, but allows decryption of data encrypted with " +
                    "either key-committing or non-key-committing algorithms. Use this during migration from older encryption.",
            CommitmentPolicy.REQUIRE_ENCRYPT_ALLOW_DECRYPT
    ),
    FORBID_ENCRYPT_ALLOW_DECRYPT(
            "Forbid Encrypt Allow Decrypt",
            "Forbids encryption but allows decryption of data encrypted with either key-committing or non-key-committing algorithms. " +
                    "Use this for read-only access to legacy encrypted data.",
            CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT
    );

    private final String displayName;
    private final String description;
    private final CommitmentPolicy commitmentPolicy;

    S3EncryptionCommitmentPolicy(final String displayName, final String description, final CommitmentPolicy commitmentPolicy) {
        this.displayName = displayName;
        this.description = description;
        this.commitmentPolicy = commitmentPolicy;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public CommitmentPolicy getCommitmentPolicy() {
        return commitmentPolicy;
    }
}

