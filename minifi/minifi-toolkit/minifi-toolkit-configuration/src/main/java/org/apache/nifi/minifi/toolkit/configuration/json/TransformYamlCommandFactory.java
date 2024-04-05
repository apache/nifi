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

package org.apache.nifi.minifi.toolkit.configuration.json;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.io.IOUtils.write;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.minifi.toolkit.configuration.ConfigMain;
import org.apache.nifi.minifi.toolkit.configuration.ConfigTransformException;
import org.apache.nifi.minifi.toolkit.configuration.PathInputStreamFactory;
import org.apache.nifi.minifi.toolkit.configuration.PathOutputStreamFactory;
import org.apache.nifi.minifi.toolkit.schema.ConfigSchema;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.toolkit.schema.common.Schema;
import org.apache.nifi.minifi.toolkit.schema.exception.SchemaInstantiatonException;
import org.apache.nifi.minifi.toolkit.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.toolkit.schema.serialization.SchemaLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransformYamlCommandFactory {

    public static final String TRANSFORM_YML = "transform-yml";

    private static final String COMMAND_DESCRIPTION = "Transform MiNiFi config YAML into NiFi flow JSON format";
    private static final String PROPERTY_KEY_VALUE_DELIMITER = "=";

    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public TransformYamlCommandFactory(PathInputStreamFactory pathInputStreamFactory, PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
    }

    public ConfigMain.Command create() {
        return new ConfigMain.Command(this::transformYamlToJson, COMMAND_DESCRIPTION);
    }

    private int transformYamlToJson(String[] args) {
        if (args.length != 5) {
            printTransformYmlUsage();
            return ConfigMain.ERR_INVALID_ARGS;
        }

        String sourceMiNiFiConfigYamlPath = args[1];
        String sourceBootstrapConfigPath = args[2];
        String targetFlowJsonPath = args[3];
        String targetBootstrapConfigPath = args[4];

        try {
            ConfigSchema configSchema = readMiNiFiConfig(sourceMiNiFiConfigYamlPath);

            Properties sourceBootstrapProperties = loadProperties(sourceBootstrapConfigPath);
            Properties targetBootstrapProperties = loadProperties(targetBootstrapConfigPath);

            ConfigSchemaToVersionedDataFlowTransformer transformer = new ConfigSchemaToVersionedDataFlowTransformer(configSchema);
            VersionedDataflow convertedFlow = transformer.convert();

            Map<String, String> extractedProperties = transformer.extractProperties();
            targetBootstrapProperties.putAll(sourceBootstrapProperties.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue)));
            targetBootstrapProperties.putAll(extractedProperties);

            persistFlowJson(convertedFlow, targetFlowJsonPath);
            persistProperties(targetBootstrapProperties, targetBootstrapConfigPath);
        } catch (ConfigTransformException e) {
            System.out.println("Unable to convert MiNiFi config YAML to flow JSON: " + e);
            return e.getErrorCode();
        }

        return ConfigMain.SUCCESS;
    }

    private void printTransformYmlUsage() {
        System.out.println("Transform YML Usage:");
        System.out.println();
        System.out.println(" transform-yml SOURCE_MINIFI_CONFIG_YAML_FILE SOURCE_BOOTSTRAP_PROPERTIES_FILE TARGET_FLOW_JSON_FILE TARGET_BOOTSTRAP_PROPERTIES_FILE");
        System.out.println();
    }

    private ConfigSchema readMiNiFiConfig(String miNiFiConfigPath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(miNiFiConfigPath)) {
            ConvertableSchema<ConfigSchema> convertableSchema = throwIfInvalid(SchemaLoader.loadConvertableSchemaFromYaml(inputStream));
            return throwIfInvalid(convertableSchema.convert());
        } catch (IOException e) {
            throw new ConfigTransformException("Error when read MiNiFi config file", ConfigMain.ERR_UNABLE_TO_READ_TEMPLATE, e);
        } catch (SchemaLoaderException e) {
            throw new ConfigTransformException("Error when parsing MiNiFi config file", ConfigMain.ERR_UNABLE_TO_PARSE_CONFIG, e);
        }
    }

    private <T extends Schema> T throwIfInvalid(T schema) throws SchemaLoaderException {
        if (!schema.isValid()) {
            throw new SchemaInstantiatonException("Failed to transform config file due to:["
                + schema.getValidationIssues().stream().sorted().collect(joining("], [")) + "]");
        }
        return schema;
    }

    private Properties loadProperties(String propertiesFilePath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(propertiesFilePath)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            throw new ConfigTransformException("Error when read properties file: " + propertiesFilePath, ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, e);
        }
    }

    private void persistFlowJson(VersionedDataflow flow, String flowJsonPath) throws ConfigTransformException {
        try (OutputStream outputStream = pathOutputStreamFactory.create(flowJsonPath)) {
            ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            objectMapper.writeValue(outputStream, flow);
        } catch (IOException e) {
            throw new ConfigTransformException("Error when persisting flow JSON file: " + flowJsonPath, ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }

    private void persistProperties(Properties properties, String bootstrapPropertiesPath) throws ConfigTransformException {
        try (OutputStream outputStream = pathOutputStreamFactory.create(bootstrapPropertiesPath)) {
            write(
                properties.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + PROPERTY_KEY_VALUE_DELIMITER + entry.getValue())
                    .sorted()
                    .collect(joining(lineSeparator())),
                outputStream,
                UTF_8
            );
        } catch (IOException e) {
            throw new ConfigTransformException("Error when persisting bootstrap properties file: " + bootstrapPropertiesPath, ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }
}
