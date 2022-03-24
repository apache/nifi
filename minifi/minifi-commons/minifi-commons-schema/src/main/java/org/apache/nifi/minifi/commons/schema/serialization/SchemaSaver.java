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

package org.apache.nifi.minifi.commons.schema.serialization;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;

public class SchemaSaver {
    public static void saveConfigSchema(ConfigSchema configSchema, OutputStream output) throws IOException {
        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yaml = new Yaml(new ConfigRepresenter(), dumperOptions);

        Map<String, Object> yamlMap = configSchema.toMap();
        try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(output)) {
            try {
                yaml.dump(yamlMap, outputStreamWriter);
            } catch (YAMLException e) {
                throw new IOException(e);
            }
        }
    }
}
