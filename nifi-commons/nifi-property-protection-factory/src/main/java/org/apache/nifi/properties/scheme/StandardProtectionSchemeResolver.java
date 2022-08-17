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
package org.apache.nifi.properties.scheme;

import org.apache.nifi.properties.SensitivePropertyProtectionException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Standard implementation of Protection Scheme Resolver using Property Protection Scheme enumeration
 */
public class StandardProtectionSchemeResolver implements ProtectionSchemeResolver {
    /**
     * Get Protection Scheme based on scheme matching one the supported Protection Property Scheme enumerated values
     *
     * @param scheme Scheme name required
     * @return Protection Scheme
     */
    @Override
    public ProtectionScheme getProtectionScheme(final String scheme) {
        Objects.requireNonNull(scheme, "Scheme required");
        return Arrays.stream(PropertyProtectionScheme.values())
                .filter(propertyProtectionScheme ->
                        propertyProtectionScheme.name().equals(scheme) || scheme.startsWith(propertyProtectionScheme.getPath())
                )
                .findFirst()
                .orElseThrow(() -> new SensitivePropertyProtectionException(String.format("Protection Scheme [%s] not supported", scheme)));
    }

    public List<String> getSupportedProtectionSchemes() {
        return Arrays.stream(PropertyProtectionScheme.values())
                .map(PropertyProtectionScheme::name)
                .collect(Collectors.toList());
    }
}
