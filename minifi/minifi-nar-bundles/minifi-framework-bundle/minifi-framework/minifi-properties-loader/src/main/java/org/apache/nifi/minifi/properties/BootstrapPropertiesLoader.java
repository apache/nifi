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

package org.apache.nifi.minifi.properties;

import static java.lang.String.format;
import static org.apache.nifi.minifi.commons.utils.SensitivePropertyUtils.MINIFI_BOOTSTRAP_SENSITIVE_KEY;
import static org.apache.nifi.minifi.commons.utils.SensitivePropertyUtils.getFormattedKey;

import java.io.File;
import org.apache.nifi.properties.AesGcmSensitivePropertyProvider;

public class BootstrapPropertiesLoader {

    public static BootstrapProperties load(File file) {
        ProtectedBootstrapProperties protectedProperties = loadProtectedProperties(file);
        if (protectedProperties.hasProtectedKeys()) {
            String sensitiveKey = protectedProperties.getApplicationProperties().getProperty(MINIFI_BOOTSTRAP_SENSITIVE_KEY);
            validateSensitiveKeyProperty(sensitiveKey);
            String keyHex = getFormattedKey(sensitiveKey);
            protectedProperties.addSensitivePropertyProvider(new AesGcmSensitivePropertyProvider(keyHex));
        }
        return protectedProperties.getUnprotectedProperties();
    }

    public static ProtectedBootstrapProperties loadProtectedProperties(File file) {
        return new ProtectedBootstrapProperties(PropertiesLoader.load(file, "Bootstrap"));
    }

    private static void validateSensitiveKeyProperty(String sensitiveKey) {
        if (sensitiveKey == null || sensitiveKey.trim().isEmpty()) {
            throw new IllegalArgumentException(format("bootstrap.conf contains protected properties but %s is not found", MINIFI_BOOTSTRAP_SENSITIVE_KEY));
        }
    }
}
