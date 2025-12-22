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

import software.amazon.encryption.s3.CommitmentPolicy;

/**
 * Specifies key parameters used by different encryption strategies.
 *
 * @param kmsId the KMS ID of the key (used by SSE-KMS and CSE-KMS)
 * @param material the key in Base64 encoded form (used by SSE-C and CSE-C)
 * @param md5 the MD5 hash of the key in Base64 encoded form (used by SSE-C)
 * @param commitmentPolicy the commitment policy for client-side encryption (used by CSE-KMS and CSE-C)
 */
public record S3EncryptionKeySpec(String kmsId, String material, String md5, CommitmentPolicy commitmentPolicy) {
}
