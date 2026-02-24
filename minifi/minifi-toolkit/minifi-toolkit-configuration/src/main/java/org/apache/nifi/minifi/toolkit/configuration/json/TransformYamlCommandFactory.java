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

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.io.IOUtils.write;

public class TransformYamlCommandFactory {

    public static final String TRANSFORM_YML = "transform-yml";

    private static final String COMMAND_DESCRIPTION = "Transform MiNiFi config YAML into NiFi flow JSON format";
    private static final String PROPERTY_KEY_VALUE_DELIMITER = "=";

    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public TransformYamlCommandFactory(final PathInputStreamFactory pathInputStreamFactory, final PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
    }

    public ConfigMain.Command create() {
        return new ConfigMain.Command(this::transformYamlToJson, COMMAND_DESCRIPTION);
    }

    private int transformYamlToJson(final String[] args) {
        if (args.length != 5) {
            printTransformYmlUsage();
            return ConfigMain.ERR_INVALID_ARGS;
        }

        final String sourceMiNiFiConfigYamlPath = args[1];
        final String sourceBootstrapConfigPath = args[2];
        final String targetFlowJsonPath = args[3];
        final String targetBootstrapConfigPath = args[4];

        try {
            final ConfigSchema configSchema = readMiNiFiConfig(sourceMiNiFiConfigYamlPath);

            final Properties sourceBootstrapProperties = loadProperties(sourceBootstrapConfigPath);
            final Properties targetBootstrapProperties = loadProperties(targetBootstrapConfigPath);

            final ConfigSchemaToVersionedDataFlowTransformer transformer = new ConfigSchemaToVersionedDataFlowTransformer(configSchema);
            final VersionedDataflow convertedFlow = transformer.convert();

            final Map<String, String> extractedProperties = transformer.extractProperties();
            targetBootstrapProperties.putAll(sourceBootstrapProperties.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue)));
            targetBootstrapProperties.putAll(extractedProperties);

            persistFlowJson(convertedFlow, targetFlowJsonPath);
            persistProperties(targetBootstrapProperties, targetBootstrapConfigPath);
        } catch (final ConfigTransformException e) {
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

    private ConfigSchema readMiNiFiConfig(final String miNiFiConfigPath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(miNiFiConfigPath)) {
            final ConvertableSchema<ConfigSchema> convertableSchema = throwIfInvalid(SchemaLoader.loadConvertableSchemaFromYaml(inputStream));
            return throwIfInvalid(convertableSchema.convert());
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when read MiNiFi config file", ConfigMain.ERR_UNABLE_TO_READ_TEMPLATE, e);
        } catch (final SchemaLoaderException e) {
            throw new ConfigTransformException("Error when parsing MiNiFi config file", ConfigMain.ERR_UNABLE_TO_PARSE_CONFIG, e);
        }
    }

    private <T extends Schema> T throwIfInvalid(final T schema) throws SchemaLoaderException {
        if (!schema.isValid()) {
            throw new SchemaInstantiatonException("Failed to transform config file due to:["
                + schema.getValidationIssues().stream().sorted().collect(joining("], [")) + "]");
        }
        return schema;
    }

    private Properties loadProperties(final String propertiesFilePath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(propertiesFilePath)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when read properties file: " + propertiesFilePath, ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, e);
        }
    }

    private void persistFlowJson(final VersionedDataflow flow, final String flowJsonPath) throws ConfigTransformException {
        try (OutputStream outputStream = pathOutputStreamFactory.create(flowJsonPath)) {
            final ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            objectMapper.writeValue(outputStream, flow);
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when persisting flow JSON file: " + flowJsonPath, ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }

    private void persistProperties(final Properties properties, final String bootstrapPropertiesPath) throws ConfigTransformException {
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
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when persisting bootstrap properties file: " + bootstrapPropertiesPath, ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }
}
