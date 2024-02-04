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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BootstrapPropertiesLoaderTest {

    private static final String NON_EXISTING_FILE_PATH = "/conf/nonexisting.conf";
    private static final String DUPLICATED_ITEMS_FILE_PATH = "/conf/bootstrap_duplicated_items.conf";
    private static final String UNPROTECTED_ITEMS_FILE_PATH = "/conf/bootstrap_unprotected.conf";
    private static final String PROTECTED_ITEMS_FILE_PATH = "/conf/bootstrap_protected.conf";
    private static final String PROTECTED_ITEMS_WITHOUT_SENSITIVE_KEY_FILE_PATH = "/conf/bootstrap_protected_without_sensitive_key.conf";

    private static final Map<String, String> UNPROTECTED_PROPERTIES = Map.of("nifi.minifi.security.keystorePasswd", "testPassword", "nifi.minifi.sensitive.props.key", "testSensitivePropsKey");

    @Test
    void shouldThrowIllegalArgumentExceptionIfFileIsNotProvided() {
        assertThrows(IllegalArgumentException.class, () -> BootstrapPropertiesLoader.load(null));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfFileDoesNotExists() {
        assertThrows(IllegalArgumentException.class, () -> BootstrapPropertiesLoader.load(new File(NON_EXISTING_FILE_PATH)));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfTheConfigFileContainsDuplicatedKeysWithDifferentValues() {
        assertThrows(IllegalArgumentException.class, () -> BootstrapPropertiesLoader.load(getFile(DUPLICATED_ITEMS_FILE_PATH)));
    }

    @Test
    void shouldReturnPropertiesIfConfigFileDoesNotContainProtectedProperties() {
        BootstrapProperties bootstrapProperties = BootstrapPropertiesLoader.load(getFile(UNPROTECTED_ITEMS_FILE_PATH));

        assertEquals(UNPROTECTED_PROPERTIES,
            bootstrapProperties.getPropertyKeys().stream().filter(UNPROTECTED_PROPERTIES::containsKey).collect(Collectors.toMap(Function.identity(), bootstrapProperties::getProperty)));
    }

    @Test
    void shouldReturnUnProtectedProperties() {
        BootstrapProperties bootstrapProperties = BootstrapPropertiesLoader.load(getFile(PROTECTED_ITEMS_FILE_PATH));

        assertEquals(UNPROTECTED_PROPERTIES,
            bootstrapProperties.getPropertyKeys().stream().filter(UNPROTECTED_PROPERTIES::containsKey).collect(Collectors.toMap(Function.identity(), bootstrapProperties::getProperty)));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfFileContainsProtectedPropertiesButSensitiveKeyIsMissing() {
        assertThrows(IllegalArgumentException.class, () -> BootstrapPropertiesLoader.load(getFile(PROTECTED_ITEMS_WITHOUT_SENSITIVE_KEY_FILE_PATH)));
    }

    private File getFile(String duplicatedItemsFilePath) {
        URL resource = BootstrapPropertiesLoaderTest.class.getResource(duplicatedItemsFilePath);
        assertNotNull(resource);
        return new File(resource.getPath());
    }
}