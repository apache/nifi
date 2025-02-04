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
package org.apache.nifi.flow.encryptor.command;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.UUID;

public class SetSensitivePropertiesKeyTest {

    @AfterEach
    public void clearProperties() {
        System.clearProperty(FlowEncryptorCommand.PROPERTIES_FILE_PATH);
    }

    @Test
    public void testMainNoArguments() {
        SetSensitivePropertiesKey.main(new String[]{});
    }

    @Test
    public void testMainPopulatedKeyAndAlgorithm(@TempDir final Path tempDir) throws IOException, URISyntaxException {
        final Path propertiesPath = FlowEncryptorCommandTest.getPopulatedNiFiProperties(tempDir);
        System.setProperty(FlowEncryptorCommand.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String sensitivePropertiesKey = UUID.randomUUID().toString();
        SetSensitivePropertiesKey.main(new String[]{sensitivePropertiesKey});

        FlowEncryptorCommandTest.assertPropertiesKeyUpdated(propertiesPath, sensitivePropertiesKey);
    }
}
