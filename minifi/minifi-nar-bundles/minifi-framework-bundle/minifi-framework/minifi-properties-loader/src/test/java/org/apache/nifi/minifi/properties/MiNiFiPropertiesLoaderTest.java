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
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;

class MiNiFiPropertiesLoaderTest {
    private static final String NON_EXISTING_FILE_PATH = "/conf/nonexisting.properties";
    private static final String DUPLICATED_ITEMS_FILE_PATH = "/conf/bootstrap_duplicated_items.conf";
    private static final String UNPROTECTED_ITEMS_FILE_PATH = "/conf/minifi_unprotected.properties";
    private static final String PROTECTED_ITEMS_FILE_PATH = "/conf/minifi_protected.properties";
    private static final Map<String, String> UNPROTECTED_PROPERTIES = Map.of("nifi.security.keystorePasswd", "testPassword", "nifi.security.keyPasswd",
        "testSensitivePropsKey");
    private static final String PROTECTION_KEY = "00714ae7a77b24cde1d36bd19472777e0d4ab02c38913b7f9bf41f3963147b4f";

    @Test
    void shouldThrowIllegalArgumentExceptionIfFileIsNotProvided() {
        MiNiFiPropertiesLoader miNiFiPropertiesLoader = new MiNiFiPropertiesLoader("");
        assertThrows(IllegalArgumentException.class, () -> miNiFiPropertiesLoader.load((String) null));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfFileDoesNotExists() {
        MiNiFiPropertiesLoader miNiFiPropertiesLoader = new MiNiFiPropertiesLoader("");
        assertThrows(IllegalArgumentException.class, () -> miNiFiPropertiesLoader.load(new File(NON_EXISTING_FILE_PATH)));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfTheConfigFileContainsDuplicatedKeysWithDifferentValues() {
        MiNiFiPropertiesLoader miNiFiPropertiesLoader = new MiNiFiPropertiesLoader("");

        assertThrows(IllegalArgumentException.class, () -> miNiFiPropertiesLoader.load(getFile(DUPLICATED_ITEMS_FILE_PATH)));
    }

    @Test
    void shouldReturnPropertiesIfConfigFileDoesNotContainProtectedProperties() {
        MiNiFiPropertiesLoader miNiFiPropertiesLoader = new MiNiFiPropertiesLoader("");

        NiFiProperties niFiProperties = miNiFiPropertiesLoader.load(getFile(UNPROTECTED_ITEMS_FILE_PATH));

        assertEquals(UNPROTECTED_PROPERTIES,
            niFiProperties.getPropertyKeys().stream().filter(UNPROTECTED_PROPERTIES::containsKey).collect(Collectors.toMap(Function.identity(), niFiProperties::getProperty)));
    }

    @Test
    void shouldReturnUnProtectedProperties() {
        MiNiFiPropertiesLoader miNiFiPropertiesLoader = new MiNiFiPropertiesLoader(PROTECTION_KEY);

        NiFiProperties niFiProperties = miNiFiPropertiesLoader.load(getUrl(PROTECTED_ITEMS_FILE_PATH).getPath());

        assertEquals(UNPROTECTED_PROPERTIES,
            niFiProperties.getPropertyKeys().stream().filter(UNPROTECTED_PROPERTIES::containsKey).collect(Collectors.toMap(Function.identity(), niFiProperties::getProperty)));
    }

    private File getFile(String propertiesFilePath) {
        URL resource = getUrl(propertiesFilePath);
        return new File(resource.getPath());
    }

    private static URL getUrl(String propertiesFilePath) {
        URL resource = BootstrapPropertiesLoaderTest.class.getResource(propertiesFilePath);
        assertNotNull(resource);
        return resource;
    }
}