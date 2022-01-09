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
package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class NamespaceResolvers implements NamespaceResolver {
    private final Set<NamespaceResolver> resolvers;

    private final String defaultNamespace;

    public NamespaceResolvers(Set<NamespaceResolver> resolvers, String defaultNamespace) {
        this.resolvers = resolvers;
        this.defaultNamespace = defaultNamespace;
    }

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (NamespaceResolver resolver : resolvers) {
            final PropertyDescriptor descriptor = resolver.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
            if (descriptor != null) {
                return descriptor;
            }
        }
        return null;
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>();
        for (NamespaceResolver resolver : resolvers) {
            results.addAll(resolver.validate(validationContext));
        }
        return results;
    }

    @Override
    public void configure(PropertyContext context) {
        for (NamespaceResolver resolver : resolvers) {
            resolver.configure(context);
        }
    }

    @Override
    public String fromHostNames(String ... hostNames) {
        for (NamespaceResolver resolver : resolvers) {
            final String namespace = resolver.fromHostNames(hostNames);
            if (namespace != null && !namespace.isEmpty()) {
                return namespace;
            }
        }
        return defaultNamespace;
    }

    @Override
    public String fromHints(Map<String, String> hints) {
        for (NamespaceResolver resolver : resolvers) {
            final String namespace = resolver.fromHints(hints);
            if (namespace != null && !namespace.isEmpty()) {
                return namespace;
            }
        }
        return defaultNamespace;
    }
}
