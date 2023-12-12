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

package org.apache.nifi.minifi.commons.utils;

import static java.lang.String.format;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class SensitivePropertyUtils {

    public static final String MINIFI_BOOTSTRAP_SENSITIVE_KEY = "minifi.bootstrap.sensitive.key";
    private static final String EMPTY = "";

    private SensitivePropertyUtils() {
    }

    public static String getFormattedKey() {
        String key = getKey(System.getProperty("minifi.bootstrap.conf.file.path"));
        // Format the key (check hex validity and remove spaces)
        return getFormattedKey(key);
    }

    public static String getFormattedKey(String unformattedKey) {
        String key = formatHexKey(unformattedKey);

        if (isNotEmpty(key) && !isHexKeyValid(key)) {
            throw new IllegalArgumentException("The key was not provided in valid hex format and of the correct length");
        } else {
            return key;
        }
    }

    private static String formatHexKey(String input) {
        return Optional.ofNullable(input)
            .map(String::trim)
            .filter(SensitivePropertyUtils::isNotEmpty)
            .map(str -> str.replaceAll("[^0-9a-fA-F]", EMPTY).toLowerCase())
            .orElse(EMPTY);
    }

    private static boolean isHexKeyValid(String key) {
        return Optional.ofNullable(key)
            .map(String::trim)
            .filter(SensitivePropertyUtils::isNotEmpty)
            .filter(k -> k.matches("^[0-9a-fA-F]{64}$"))
            .isPresent();
    }

    private static String getKey(String bootstrapConfigFilePath) {
        Properties properties = new Properties();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(bootstrapConfigFilePath))) {
            properties.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(format("Loading Bootstrap Properties [%s] failed", bootstrapConfigFilePath), e);
        }

        return properties.getProperty(MINIFI_BOOTSTRAP_SENSITIVE_KEY);
    }

    private static boolean isNotEmpty(String keyFilePath) {
        return keyFilePath != null && !keyFilePath.isBlank();
    }
}
