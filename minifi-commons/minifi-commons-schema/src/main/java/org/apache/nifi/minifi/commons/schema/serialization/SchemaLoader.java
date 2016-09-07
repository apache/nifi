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
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SchemaLoader {
    private static final Map<String, Function<Map, ConfigSchema>> configSchemaFactories = initConfigSchemaFactories();

    private static Map<String, Function<Map, ConfigSchema>> initConfigSchemaFactories() {
        Map<String, Function<Map, ConfigSchema>> result = new HashMap<>();
        result.put(String.valueOf((Object)null), ConfigSchema::new);
        result.put("", ConfigSchema::new);
        result.put(Integer.toString(ConfigSchema.CONFIG_VERSION), ConfigSchema::new);
        return result;
    }


    public static Map<String, Object> loadYamlAsMap(InputStream sourceStream) throws IOException, SchemaLoaderException {
        try {
            Yaml yaml = new Yaml();

            // Parse the YAML file
            final Object loadedObject = yaml.load(sourceStream);

            // Verify the parsed object is a Map structure
            if (loadedObject instanceof Map) {
                return (Map<String, Object>) loadedObject;
            } else {
                throw new SchemaLoaderException("Provided YAML configuration is not a Map");
            }
        } catch (YAMLException e) {
            throw new IOException(e);
        } finally {
            sourceStream.close();
        }
    }

    public static ConfigSchema loadConfigSchemaFromYaml(InputStream sourceStream) throws IOException, SchemaLoaderException {
        return loadConfigSchemaFromYaml(loadYamlAsMap(sourceStream));
    }

    public static ConfigSchema loadConfigSchemaFromYaml(Map<String, Object> yamlAsMap) throws SchemaLoaderException {
        String version = String.valueOf(yamlAsMap.get(ConfigSchema.VERSION));
        Function<Map, ConfigSchema> schemaFactory = configSchemaFactories.get(version);
        if (schemaFactory == null) {
            throw new SchemaLoaderException("YAML configuration version " + version + " not supported.  Supported versions: "
                    + configSchemaFactories.keySet().stream().sorted().collect(Collectors.joining(", ")));
        }
        return schemaFactory.apply(yamlAsMap);
    }
}
