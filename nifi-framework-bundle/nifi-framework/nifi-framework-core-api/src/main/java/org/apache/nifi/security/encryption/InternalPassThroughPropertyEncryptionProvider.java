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

/**
 * Internal Pass Through implementation of Property Encryption Provider that does not modify property values.
 *
 * <p>Intended for flows that are mapped and synchronized in memory rather than persisted. Sensitive values still pass
 * through {@link SensitivePropertyCodec}, which represents them as hexadecimal, so a flow mapped using this Provider
 * must also be synchronized using this Provider in order to recover the original values.</p>
 */
public class InternalPassThroughPropertyEncryptionProvider implements PropertyEncryptionProvider {
    @Override
    public void initialize(final PropertyEncryptionProviderInitializationContext context) {

    }

    @Override
    public byte[] encrypt(final byte[] property, final SensitivePropertyContext context) {
        return property;
    }

    @Override
    public byte[] decrypt(final byte[] encryptedProperty, final SensitivePropertyContext context) {
        return encryptedProperty;
    }
}
