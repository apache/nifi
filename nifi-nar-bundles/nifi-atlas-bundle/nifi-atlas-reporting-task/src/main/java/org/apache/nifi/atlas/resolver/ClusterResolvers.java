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

public class ClusterResolvers implements ClusterResolver {
    private final Set<ClusterResolver> resolvers;

    private final String defaultClusterName;

    public ClusterResolvers(Set<ClusterResolver> resolvers, String defaultClusterName) {
        this.resolvers = resolvers;
        this.defaultClusterName = defaultClusterName;
    }

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (ClusterResolver resolver : resolvers) {
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
        for (ClusterResolver resolver : resolvers) {
            results.addAll(resolver.validate(validationContext));
        }
        return results;
    }

    @Override
    public void configure(PropertyContext context) {
        for (ClusterResolver resolver : resolvers) {
            resolver.configure(context);
        }
    }

    @Override
    public String fromHostNames(String ... hostNames) {
        for (ClusterResolver resolver : resolvers) {
            final String clusterName = resolver.fromHostNames(hostNames);
            if (clusterName != null && !clusterName.isEmpty()) {
                return clusterName;
            }
        }
        return defaultClusterName;
    }

    @Override
    public String fromHints(Map<String, String> hints) {
        for (ClusterResolver resolver : resolvers) {
            final String clusterName = resolver.fromHints(hints);
            if (clusterName != null && !clusterName.isEmpty()) {
                return clusterName;
            }
        }
        return defaultClusterName;
    }
}
