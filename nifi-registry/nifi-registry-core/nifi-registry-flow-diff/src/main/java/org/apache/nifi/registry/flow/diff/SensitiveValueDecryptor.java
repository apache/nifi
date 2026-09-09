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
package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedComponent;

/**
 * Decrypts a sensitive value so that two flows can be compared on the decrypted values.
 *
 * <p>The owning component and the name of the value are supplied because an implementation may need to describe the
 * location of a value in order to decrypt it.</p>
 */
@FunctionalInterface
public interface SensitiveValueDecryptor {
    /**
     * Decrypt a sensitive value
     *
     * @param owner Component that owns the value, which is the Parameter Context for a Parameter value
     * @param valueName Name of the property or Parameter that holds the value
     * @param encryptedValue Encrypted value without the surrounding encryption markers
     * @return Decrypted value
     */
    String decrypt(VersionedComponent owner, String valueName, String encryptedValue);
}
