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
package org.apache.nifi.processors.standard.crypto.attributes;

/**
 * Cryptographic Flow File Attribute for reference in Processor documentation
 */
public interface CryptographicAttribute {
    String ALGORITHM = "cryptographic.algorithm";

    String ALGORITHM_CIPHER = "cryptographic.algorithm.cipher";

    String ALGORITHM_KEY_SIZE = "cryptographic.algorithm.key.size";

    String ALGORITHM_BLOCK_CIPHER_MODE = "cryptographic.algorithm.block.cipher.mode";

    String ALGORITHM_OBJECT_IDENTIFIER = "cryptographic.algorithm.object.identifier";

    String METHOD = "cryptographic.method";

    String PROCESSING_COMPLETED = "cryptographic.processing.completed";
}
