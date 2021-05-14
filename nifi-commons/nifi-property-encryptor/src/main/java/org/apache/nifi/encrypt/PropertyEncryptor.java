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
package org.apache.nifi.encrypt;

/**
 * Property Encryptor supporting encryption and decryption using configurable algorithms and keys
 */
public interface PropertyEncryptor {
    /**
     * Encrypt property value and return string representation of enciphered contents
     *
     * @param property Property value to be encrypted
     * @return Encrypted representation of property value string
     */
    String encrypt(String property);

    /**
     * Decrypt encrypted property value and return deciphered contents
     *
     * @param encryptedProperty Encrypted property value to be deciphered
     * @return Decrypted property value string
     */
    String decrypt(String encryptedProperty);
}
