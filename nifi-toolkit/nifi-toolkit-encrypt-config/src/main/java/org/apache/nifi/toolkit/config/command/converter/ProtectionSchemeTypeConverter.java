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
package org.apache.nifi.toolkit.config.command.converter;

import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.ProtectionSchemeResolver;
import org.apache.nifi.properties.scheme.StandardProtectionSchemeResolver;
import picocli.CommandLine;

/**
 * Argument Type Converter implementation for Property Protection Schemes
 */
public class ProtectionSchemeTypeConverter implements CommandLine.ITypeConverter<ProtectionScheme> {
    private static final ProtectionSchemeResolver PROTECTION_SCHEME_RESOLVER = new StandardProtectionSchemeResolver();

    /**
     * Convert value to Property Protection Scheme using Resolver based on enumerated name or matching path string
     *
     * @param value Argument value to be converted
     * @return Protection Scheme resolved from argument value provided
     */
    @Override
    public ProtectionScheme convert(final String value) {
        return PROTECTION_SCHEME_RESOLVER.getProtectionScheme(value);
    }
}
