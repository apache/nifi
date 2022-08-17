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
package org.apache.nifi;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NiFiTest {
    private static final String[] ARGUMENTS = new String[]{};

    private static final String KEY_ARGUMENT = "-K";

    private static final String[] FILE_NOT_SPECIFIED_ARGUMENTS = new String[]{ KEY_ARGUMENT };

    private static final String FAILURE_TO_LAUNCH = "Failure to launch NiFi";

    private static final String PROPERTIES_LOADED = "Application Properties loaded";

    private static final String PROPERTIES_PATH = "/NiFiProperties/conf/nifi.properties";

    private static final String ENCRYPTED_PROPERTIES_PATH = "/NiFiProperties/conf/encrypted.nifi.properties";

    private static final String ROOT_KEY = StringUtils.repeat("0", 64);

    @BeforeEach
    public void setAppender() {
        ListAppender.clear();
    }

    @AfterEach
    public void clearPropertiesFilePath() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, StringUtils.EMPTY);
    }

    @Test
    public void testMainBootstrapKeyFileNotSpecified() {
        setPropertiesFilePath(PROPERTIES_PATH);

        NiFi.main(FILE_NOT_SPECIFIED_ARGUMENTS);

        assertFailureToLaunch();
    }

    @Test
    public void testMainBootstrapKeyNotSpecified() {
        setPropertiesFilePath(PROPERTIES_PATH);

        NiFi.main(ARGUMENTS);

        assertFailureToLaunch();
    }

    @Test
    public void testMainEncryptedNiFiProperties() throws IOException {
        final File rootKeyFile = File.createTempFile(getClass().getSimpleName(), ".root.key");
        rootKeyFile.deleteOnExit();
        try (final PrintWriter writer = new PrintWriter(new FileWriter(rootKeyFile))) {
            writer.println(ROOT_KEY);
        }

        setPropertiesFilePath(ENCRYPTED_PROPERTIES_PATH);

        NiFi.main(new String[]{ KEY_ARGUMENT, rootKeyFile.getAbsolutePath() });

        assertApplicationPropertiesLoaded();
        assertFailureToLaunch();
    }

    private void assertApplicationPropertiesLoaded() {
        final Optional<LoggingEvent> event = ListAppender.getLoggingEvents().stream().filter(
                loggingEvent -> loggingEvent.getMessage().startsWith(PROPERTIES_LOADED)
        ).findFirst();
        assertTrue(event.isPresent(), "Properties loaded log not found");
    }

    private void assertFailureToLaunch() {
        final Optional<LoggingEvent> event = ListAppender.getLoggingEvents().stream().filter(
                loggingEvent -> loggingEvent.getMessage().startsWith(FAILURE_TO_LAUNCH)
        ).findFirst();
        assertTrue(event.isPresent(), "Failure log not found");
    }

    private void setPropertiesFilePath(final String relativePath) {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, getResourcesPath(relativePath));
    }

    private String getResourcesPath(final String relativePath) {
        return getClass().getResource(relativePath).getPath();
    }
}
