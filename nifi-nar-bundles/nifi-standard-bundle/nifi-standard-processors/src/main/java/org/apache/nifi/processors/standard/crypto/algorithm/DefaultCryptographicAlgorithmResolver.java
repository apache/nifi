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
package org.apache.nifi.processors.standard.crypto.algorithm;

import java.util.Arrays;
import java.util.Optional;

/**
 * Default implementation of Cryptographic Algorithm Resolver
 */
public class DefaultCryptographicAlgorithmResolver implements CryptographicAlgorithmResolver {
    /**
     * Find Cryptographic Algorithm based on Object Identifier using Cryptographic Algorithm enumeration
     *
     * @param objectIdentifier ASN.1 Object Identifier
     * @return Cryptographic Algorithm or empty when not found
     */
    @Override
    public Optional<CryptographicAlgorithm> findCryptographicAlgorithm(final String objectIdentifier) {
        return Arrays.stream(CryptographicAlgorithm.values()).filter(
                method -> method.getObjectIdentifier().equals(objectIdentifier)
        ).findFirst();
    }
}
