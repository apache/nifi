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
package org.apache.nifi.security.crypto.key.detection;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.bcrypt.BcryptDerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.bcrypt.BcryptDerivedKeyProvider;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Provider delegating to configured implementations based on Parameter Specification class
 */
public class DetectedDerivedKeyProvider implements DerivedKeyProvider<DerivedKeyParameterSpec> {
    private static final Map<Class<? extends DerivedKeyParameterSpec>, DerivedKeyProvider<? extends DerivedKeyParameterSpec>> providers = new LinkedHashMap<>();

    static {
        providers.put(Argon2DerivedKeyParameterSpec.class, new Argon2DerivedKeyProvider());
        providers.put(BcryptDerivedKeyParameterSpec.class, new BcryptDerivedKeyProvider());
        providers.put(ScryptDerivedKeyParameterSpec.class, new ScryptDerivedKeyProvider());
        providers.put(Pbkdf2DerivedKeyParameterSpec.class, new Pbkdf2DerivedKeyProvider());
    }

    /**
     * Get Derived Key using implementation selected based on assignable Parameter Specification class mapped to Provider
     *
     * @param derivedKeySpec Derived Key Specification
     * @return Derived Key
     */
    @Override
    public DerivedKey getDerivedKey(final DerivedKeySpec<DerivedKeyParameterSpec> derivedKeySpec) {
        Objects.requireNonNull(derivedKeySpec, "Specification required");

        final Class<? extends DerivedKeyParameterSpec> parameterSpecClass = derivedKeySpec.getParameterSpec().getClass();
        final DerivedKeyProvider<DerivedKeyParameterSpec> derivedKeyProvider = findProvider(parameterSpecClass);

        return derivedKeyProvider.getDerivedKey(derivedKeySpec);
    }

    @SuppressWarnings("unchecked")
    private DerivedKeyProvider<DerivedKeyParameterSpec> findProvider(final Class<? extends DerivedKeyParameterSpec> parameterSpecClass) {
        final Class<? extends DerivedKeyParameterSpec> foundSpecClass = providers.keySet()
                .stream()
                .filter(specClass -> specClass.isAssignableFrom(parameterSpecClass))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException(String.format("Parameter Specification [%s] not supported", parameterSpecClass)));

        final DerivedKeyProvider<? extends DerivedKeyParameterSpec> derivedKeyProvider = providers.get(foundSpecClass);
        return (DerivedKeyProvider<DerivedKeyParameterSpec>) derivedKeyProvider;
    }
}
