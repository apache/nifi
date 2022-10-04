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
package org.apache.nifi.security.util;

import java.util.Arrays;
import java.util.Objects;

public class StandardEncryptionMethodResolver implements EncryptionMethodResolver {
    /**
     * Get Protection Scheme based on algorithm matching one the supported Protection Property Scheme enumerated values
     *
     * @param algorithm Scheme name required
     * @return Protection Scheme
     */
    @Override
    public EncryptionMethod getEncryptionMethod(String algorithm) {
        Objects.requireNonNull(algorithm, "Algorithm required");
        return Arrays.stream(EncryptionMethod.values())
                .filter(encryptionMethod ->
                        encryptionMethod.name().equals(algorithm))
                .findFirst()
                .orElseThrow(() -> new EncryptionMethodException(String.format("Encryption algorithm [%s] not supported", algorithm)));
    }
}
