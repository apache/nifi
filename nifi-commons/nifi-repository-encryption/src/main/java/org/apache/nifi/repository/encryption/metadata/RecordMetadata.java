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
package org.apache.nifi.repository.encryption.metadata;

/**
 * Metadata descriptor for encrypted records
 */
public interface RecordMetadata {
    /**
     * Get key identifier
     *
     * @return Key identifier used for encryption
     */
    String getKeyId();

    /**
     * Get initialization vector
     *
     * @return Initialization vector used for encryption
     */
    byte[] getInitializationVector();

    /**
     * Get length of encrypted binary
     *
     * @return Length of encrypted binary or -1 when unknown for streams
     */
    int getLength();

    /**
     * Get cipher algorithm
     *
     * @return Cipher algorithm
     */
    String getAlgorithm();

    /**
     * Get metadata version
     *
     * @return Metadata version
     */
    String getVersion();
}
