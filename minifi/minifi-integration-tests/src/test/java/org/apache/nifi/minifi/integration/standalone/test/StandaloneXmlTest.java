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

package org.apache.nifi.minifi.integration.standalone.test;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaSaver;
import org.apache.nifi.minifi.toolkit.configuration.ConfigMain;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class StandaloneXmlTest extends StandaloneYamlTest {
    public void setDocker(String version, String name) throws Exception {
        ConfigSchema configSchema;
        try (InputStream inputStream = StandaloneXmlTest.class.getClassLoader().getResourceAsStream("./standalone/" + version + "/" + name + "/xml/" + name + ".xml")) {
            configSchema = ConfigMain.transformTemplateToSchema(inputStream);
        }
        try (OutputStream outputStream = Files.newOutputStream(Paths.get(StandaloneXmlTest.class.getClassLoader().getResource("docker-compose-v1-standalone.yml").getFile())
                .getParent().toAbsolutePath().resolve(getConfigYml(version, name)))) {
            SchemaSaver.saveConfigSchema(configSchema, outputStream);
        }
        super.setDocker(version, name);
    }

    @Override
    protected String getConfigYml(final String version, final String name) {
        return "./standalone/" + version + "/" + name + "/xml/" + name + ".yml";
    }

    @Override
    protected String getExpectedJson(final String version, final String name) {
        return "standalone/" + version + "/" + name + "/xml/expected.json";
    }
}
