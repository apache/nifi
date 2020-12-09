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

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.BOOTSTRAP_KEYS_TO_YML_KEYS;
import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.BOOTSTRAP_PROVENANCE_REPORTING_KEYS;
import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.BOOTSTRAP_SECURITY_PROPERTY_KEYS;
import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.BOOTSTRAP_SENSITIVE_PROPERTY_KEYS;
import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.USE_PARENT_SSL;

public class BootstrapTransformer {

    public static Optional<SecurityPropertiesSchema> buildSecurityPropertiesFromBootstrap(final Properties bootstrapProperties) {

        Optional<SecurityPropertiesSchema> securityPropsOptional = Optional.empty();

        final Map<String, Object> securityProperties = new HashMap<>();

        if (bootstrapProperties != null) {
            BOOTSTRAP_SECURITY_PROPERTY_KEYS.stream()
                    .filter(key -> StringUtils.isNotBlank(bootstrapProperties.getProperty(key)))
                    .forEach(key ->
                            securityProperties.put(BOOTSTRAP_KEYS_TO_YML_KEYS.get(key), bootstrapProperties.getProperty(key))
                    );

            if (!securityProperties.isEmpty()) {
                // Determine if sensitive properties were provided
                final Map<String, String> sensitiveProperties = new HashMap<>();
                BOOTSTRAP_SENSITIVE_PROPERTY_KEYS.stream()
                        .filter(key -> StringUtils.isNotBlank(bootstrapProperties.getProperty(key)))
                        .forEach(key ->
                                sensitiveProperties.put(BOOTSTRAP_KEYS_TO_YML_KEYS.get(key), bootstrapProperties.getProperty(key))
                        );
                if (!sensitiveProperties.isEmpty()) {
                    securityProperties.put(CommonPropertyKeys.SENSITIVE_PROPS_KEY, sensitiveProperties);
                }

                final SecurityPropertiesSchema securityPropertiesSchema = new SecurityPropertiesSchema(securityProperties);
                securityPropsOptional = Optional.of(securityPropertiesSchema);
            }
        }

        return securityPropsOptional;
    }

    public static Optional<ProvenanceReportingSchema> buildProvenanceReportingPropertiesFromBootstrap(final Properties bootstrapProperties) {

        Optional<ProvenanceReportingSchema> provenanceReportingPropsOptional = Optional.empty();

        final Map<String, Object> provenanceReportingProperties = new HashMap<>();
        if (bootstrapProperties != null) {
            BOOTSTRAP_PROVENANCE_REPORTING_KEYS.stream()
                    .filter(key -> StringUtils.isNotBlank(bootstrapProperties.getProperty(key)))
                    .forEach(key ->
                            provenanceReportingProperties.put(BOOTSTRAP_KEYS_TO_YML_KEYS.get(key), bootstrapProperties.getProperty(key))
                    );

            if (!provenanceReportingProperties.isEmpty()) {
                final ProvenanceReportingSchema provenanceReportingSchema = new ProvenanceReportingSchema(provenanceReportingProperties);
                provenanceReportingPropsOptional = Optional.of(provenanceReportingSchema);
            }
        }

        return provenanceReportingPropsOptional;
    }

    public static boolean processorSSLOverride(final Properties bootstrapProperties) {
        boolean shouldOverride = false;

        if (bootstrapProperties != null) {
            shouldOverride = Boolean.parseBoolean(bootstrapProperties.getProperty(USE_PARENT_SSL));
        }

        return shouldOverride;
    }

}
