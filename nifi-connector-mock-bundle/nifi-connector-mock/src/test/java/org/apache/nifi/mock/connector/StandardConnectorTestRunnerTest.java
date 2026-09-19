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

package org.apache.nifi.mock.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardConnectorTestRunnerTest {

    @TempDir
    private Path tempDirectory;

    @Test
    void testInstancePropertiesEmptyWithoutInstanceDirectory() {
        final Properties properties = StandardConnectorTestRunner.getInstanceProperties(null);

        assertTrue(properties.isEmpty());
    }

    @Test
    void testInstancePropertiesDerivedFromInstanceDirectory() {
        final Path instanceDirectory = tempDirectory.resolve("runner").toAbsolutePath().normalize();

        final Properties properties = StandardConnectorTestRunner.getInstanceProperties(instanceDirectory);

        final Map<String, String> expectedProperties = Map.ofEntries(
                Map.entry("nifi.flow.configuration.file", instanceDirectory.resolve("conf/flow.json.gz").toString()),
                Map.entry("nifi.flow.configuration.archive.dir", instanceDirectory.resolve("conf/archive").toString()),
                Map.entry("nifi.state.management.configuration.file", instanceDirectory.resolve("conf/state-management.xml").toString()),
                Map.entry("nifi.database.directory", instanceDirectory.resolve("database_repository").toString()),
                Map.entry("nifi.flowfile.repository.directory", instanceDirectory.resolve("flowfile_repository").toString()),
                Map.entry("nifi.content.repository.directory.default", instanceDirectory.resolve("content_repository").toString()),
                Map.entry("nifi.nar.persistence.provider.properties.directory", instanceDirectory.resolve("nar_repository").toString()),
                Map.entry("nifi.asset.manager.properties.directory", instanceDirectory.resolve("assets").toString()),
                Map.entry("nifi.connector.asset.manager.properties.directory", instanceDirectory.resolve("connector-assets").toString()),
                Map.entry("nifi.nar.working.directory", instanceDirectory.resolve("work").toString()),
                Map.entry("nifi.nar.library.autoload.directory", instanceDirectory.resolve("autoload").toString()),
                Map.entry("nifi.web.jetty.working.directory", instanceDirectory.resolve("work/jetty").toString())
        );

        assertEquals(expectedProperties, properties);
    }
}
