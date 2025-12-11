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
package org.apache.nifi.kafka.shared.util;

/**
 * Utility class to handle SASL Extension properties
 */
public class SaslExtensionUtil {

    public static final String SASL_EXTENSION_PROPERTY_PREFIX = "sasl_extension_";

    private SaslExtensionUtil() {
    }

    public static boolean isSaslExtensionProperty(final String propertyName) {
        return propertyName.startsWith(SASL_EXTENSION_PROPERTY_PREFIX);
    }

    public static String removeSaslExtensionPropertyPrefix(final String propertyName) {
        return propertyName.substring(SASL_EXTENSION_PROPERTY_PREFIX.length());
    }
}
