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

package org.apache.nifi.components.connector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Framework-managed MDC logging attribute keys for a connector. The framework computes these
 * automatically and they cannot be overridden by a connector's custom attributes.
 */
public enum ConnectorLoggingAttribute {
    CONNECTOR_ID("connectorId"),
    CONNECTOR_NAME("connectorName"),
    CONNECTOR_COMPONENT("connectorComponent"),
    CONNECTOR_BUNDLE_GROUP("connectorBundleGroup"),
    CONNECTOR_BUNDLE_ARTIFACT("connectorBundleArtifact"),
    CONNECTOR_BUNDLE_VERSION("connectorBundleVersion");

    private static final Set<String> RESERVED_KEYS;
    static {
        final Set<String> reserved = new HashSet<>();
        for (final ConnectorLoggingAttribute attribute : values()) {
            reserved.add(attribute.attribute);
        }
        RESERVED_KEYS = Collections.unmodifiableSet(reserved);
    }

    private final String attribute;

    ConnectorLoggingAttribute(final String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }

    public static boolean isReserved(final String key) {
        return RESERVED_KEYS.contains(key);
    }

    public static Set<String> getReservedKeys() {
        return RESERVED_KEYS;
    }
}
