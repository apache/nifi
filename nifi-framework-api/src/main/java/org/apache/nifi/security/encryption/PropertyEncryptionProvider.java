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
package org.apache.nifi.security.encryption;

import java.io.Closeable;
import java.io.IOException;

/**
 * Framework extension point for protecting sensitive values stored in the flow configuration. Implementations must be
 * thread safe, because the framework holds a single shared instance and invokes it from multiple threads.
 */
public interface PropertyEncryptionProvider extends Closeable {
    /**
     * Configuration lifecycle method that the framework invokes after instantiating the class and before requesting
     * cipher operations
     *
     * @param context Initialization context containing configured properties
     * @throws PropertyEncryptionException Thrown when initialization fails
     */
    void initialize(PropertyEncryptionProviderInitializationContext context);

    /**
     * Encrypt a sensitive value using the supplied context
     *
     * @param property Sensitive value to be encrypted
     * @param context Context describing the value being protected
     * @return Encrypted value
     * @throws PropertyEncryptionException Thrown when encryption fails
     */
    byte[] encrypt(byte[] property, SensitivePropertyContext context);

    /**
     * Decrypt a sensitive value using the context supplied when the value was encrypted
     *
     * @param encryptedProperty Encrypted value to be decrypted
     * @param context Context describing the value being protected
     * @return Decrypted value
     * @throws PropertyEncryptionException Thrown when decryption fails
     */
    byte[] decrypt(byte[] encryptedProperty, SensitivePropertyContext context);

    /**
     * Close resources created during provider initialization and processing
     *
     * @throws IOException Thrown when closing resources fails
     */
    @Override
    default void close() throws IOException {
    }
}
