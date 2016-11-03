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

package org.apache.nifi.minifi.toolkit.configuration;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaSaver;
import org.apache.nifi.minifi.toolkit.configuration.dto.ConfigSchemaFunction;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.NiFiComponentDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;

public class ConfigMain {
    public static final int ERR_INVALID_ARGS = 1;
    public static final int ERR_UNABLE_TO_OPEN_OUTPUT = 2;
    public static final int ERR_UNABLE_TO_OPEN_INPUT = 3;
    public static final int ERR_UNABLE_TO_READ_TEMPLATE = 4;
    public static final int ERR_UNABLE_TO_PARSE_CONFIG = 6;
    public static final int ERR_INVALID_CONFIG = 7;
    public static final int ERR_UNABLE_TO_CLOSE_CONFIG = 8;
    public static final int ERR_UNABLE_TO_SAVE_CONFIG = 9;

    public static final int SUCCESS = 0;

    public static final String TRANSFORM = "transform";
    public static final String VALIDATE = "validate";
    public static final String UPGRADE = "upgrade";
    public static final String THERE_ARE_VALIDATION_ERRORS_WITH_THE_TEMPLATE_STILL_OUTPUTTING_YAML_BUT_IT_WILL_NEED_TO_BE_EDITED =
            "There are validation errors with the template, still outputting YAML but it will need to be edited.";

    private final Map<String, Command> commandMap;
    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public ConfigMain() {
        this(FileInputStream::new, FileOutputStream::new);
    }

    public ConfigMain(PathInputStreamFactory pathInputStreamFactory, PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
        this.commandMap = createCommandMap();
    }

    public static void main(String[] args) {
        System.exit(new ConfigMain().execute(args));
    }

    public static void printValidateUsage() {
        System.out.println("Validate Usage:");
        System.out.println();
        System.out.println(" validate INPUT_FILE");
        System.out.println();
    }

    public int validate(String[] args) {
        if (args.length != 2) {
            printValidateUsage();
            return ERR_INVALID_ARGS;
        }
        try (InputStream inputStream = pathInputStreamFactory.create(args[1])) {
            try {
                return loadAndPrintValidationErrors(inputStream, (configSchema, valid) -> {
                    if (valid) {
                        return SUCCESS;
                    } else {
                        return ERR_INVALID_CONFIG;
                    }
                });
            } catch (IOException|SchemaLoaderException e) {
                return handleErrorLoadingConfiguration(e, ConfigMain::printValidateUsage);
            }
        } catch (FileNotFoundException e) {
            return handleErrorOpeningInput(args[1], ConfigMain::printValidateUsage, e);
        } catch (IOException e) {
            handleErrorClosingInput(e);
            return ERR_UNABLE_TO_CLOSE_CONFIG;
        }
    }

    public static void printTransformUsage() {
        System.out.println("Transform Usage:");
        System.out.println();
        System.out.println(" transform INPUT_FILE OUTPUT_FILE");
        System.out.println();
    }

    public static void printUpgradeUsage() {
        System.out.println("Upgrade Usage:");
        System.out.println();
        System.out.println(" upgrade INPUT_FILE OUTPUT_FILE");
        System.out.println();
    }

    private static void enrichFlowSnippetDTO(FlowSnippetDTO flowSnippetDTO) {
        List<FlowSnippetDTO> allFlowSnippets = getAllFlowSnippets(flowSnippetDTO);

        Set<RemoteProcessGroupDTO> remoteProcessGroups = getAll(allFlowSnippets, FlowSnippetDTO::getRemoteProcessGroups).collect(Collectors.toSet());

        // RPGs with no name get Target URI as name
        remoteProcessGroups.stream().filter(r -> StringUtil.isNullOrEmpty(r.getName())).forEach(r -> r.setName(r.getTargetUri()));

        Map<String, String> connectableNameMap = getAll(allFlowSnippets, FlowSnippetDTO::getProcessors).collect(Collectors.toMap(NiFiComponentDTO::getId, ProcessorDTO::getName));

        for (RemoteProcessGroupDTO remoteProcessGroupDTO : remoteProcessGroups) {
            RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();
            addConnectables(connectableNameMap, nullToEmpty(contents.getInputPorts()), RemoteProcessGroupPortDTO::getId, RemoteProcessGroupPortDTO::getId);
            addConnectables(connectableNameMap, nullToEmpty(contents.getOutputPorts()), RemoteProcessGroupPortDTO::getId, RemoteProcessGroupPortDTO::getId);
        }

        addConnectables(connectableNameMap, getAll(allFlowSnippets, FlowSnippetDTO::getInputPorts).collect(Collectors.toList()), PortDTO::getId, PortDTO::getName);
        addConnectables(connectableNameMap, getAll(allFlowSnippets, FlowSnippetDTO::getOutputPorts).collect(Collectors.toList()), PortDTO::getId, PortDTO::getName);

        Set<ConnectionDTO> connections = getAll(allFlowSnippets, FlowSnippetDTO::getConnections).collect(Collectors.toSet());
        for (ConnectionDTO connection : connections) {
            setName(connectableNameMap, connection.getSource());
            setName(connectableNameMap, connection.getDestination());
        }

        for (ConnectionDTO connection : connections) {
            if (StringUtil.isNullOrEmpty(connection.getName())) {
                StringBuilder name = new StringBuilder();
                ConnectableDTO connectionSource = connection.getSource();
                if (connectionSource != null) {
                    String connectionSourceName = connectionSource.getName();
                    name.append(StringUtil.isNullOrEmpty(connectionSourceName) ? connectionSource.getId() : connectionSourceName);
                }
                name.append("/");
                if (connection.getSelectedRelationships() != null && connection.getSelectedRelationships().size() > 0) {
                    name.append(connection.getSelectedRelationships().iterator().next());
                }
                name.append("/");
                ConnectableDTO connectionDestination = connection.getDestination();
                if (connectionDestination != null) {
                    String connectionDestinationName = connectionDestination.getName();
                    name.append(StringUtil.isNullOrEmpty(connectionDestinationName) ? connectionDestination.getId() : connectionDestinationName);
                }
                connection.setName(name.toString());
            }
        }
        nullToEmpty(flowSnippetDTO.getProcessGroups()).stream().map(ProcessGroupDTO::getContents).forEach(ConfigMain::enrichFlowSnippetDTO);
    }

    private static <T> Stream<T> getAll(List<FlowSnippetDTO> allFlowSnippets, Function<FlowSnippetDTO, Collection<T>> accessor) {
        return allFlowSnippets.stream().flatMap(f -> accessor.apply(f).stream()).filter(Objects::nonNull);
    }

    private static List<FlowSnippetDTO> getAllFlowSnippets(FlowSnippetDTO flowSnippetDTO) {
        List<FlowSnippetDTO> result = new ArrayList<>();
        getAllFlowSnippets(flowSnippetDTO, result);
        return result;
    }

    private static void getAllFlowSnippets(FlowSnippetDTO flowSnippetDTO, List<FlowSnippetDTO> result) {
        result.add(flowSnippetDTO);
        nullToEmpty(flowSnippetDTO.getProcessGroups()).stream().map(ProcessGroupDTO::getContents).forEach(f -> getAllFlowSnippets(f, result));
    }

    public static ConfigSchema transformTemplateToSchema(InputStream source) throws JAXBException, IOException {
        try {
            TemplateDTO templateDTO = (TemplateDTO) JAXBContext.newInstance(TemplateDTO.class).createUnmarshaller().unmarshal(source);
            enrichFlowSnippetDTO(templateDTO.getSnippet());
            ConfigSchema configSchema = new ConfigSchemaFunction().apply(templateDTO);
            return configSchema;
        } finally {
            source.close();
        }
    }

    private static void setName(Map<String, String> connectableNameMap, ConnectableDTO connectableDTO) {
        if (connectableDTO != null) {
            String name = connectableNameMap.get(connectableDTO.getId());
            if (name != null) {
                connectableDTO.setName(name);
            }
        }
    }

    private static <T> void addConnectables(Map<String, String> connectableNameMap, Collection<T> hasIdAndNames, Function<T, String> idGetter, Function<T, String> nameGetter) {
        if (hasIdAndNames != null) {
            for (T hasIdAndName : hasIdAndNames) {
                String id = idGetter.apply(hasIdAndName);
                String name = nameGetter.apply(hasIdAndName);
                if (!StringUtil.isNullOrEmpty(name)) {
                    connectableNameMap.put(id, name);
                }
            }
        }
    }

    public int upgrade(String[] args) {
        if (args.length != 3) {
            printUpgradeUsage();
            return ERR_INVALID_ARGS;
        }

        ConfigSchema currentSchema = null;
        try (InputStream inputStream = pathInputStreamFactory.create(args[1])) {
            try {
                currentSchema = loadAndPrintValidationErrors(inputStream, (configSchema, valid) -> {
                    if (!valid) {
                        System.out.println(THERE_ARE_VALIDATION_ERRORS_WITH_THE_TEMPLATE_STILL_OUTPUTTING_YAML_BUT_IT_WILL_NEED_TO_BE_EDITED);
                        System.out.println();
                    }
                    return configSchema;
                });
            } catch (IOException|SchemaLoaderException e) {
                return handleErrorLoadingConfiguration(e, ConfigMain::printUpgradeUsage);
            }
        } catch (FileNotFoundException e) {
            return handleErrorOpeningInput(args[1], ConfigMain::printUpgradeUsage, e);
        } catch (IOException e) {
            handleErrorClosingInput(e);
        }

        try (OutputStream fileOutputStream = pathOutputStreamFactory.create(args[2])) {
            try {
                SchemaSaver.saveConfigSchema(currentSchema, fileOutputStream);
            } catch (IOException e) {
                return handleErrorSavingCofiguration(e);
            }
        } catch (FileNotFoundException e) {
            return handleErrorOpeningOutput(args[2], ConfigMain::printUpgradeUsage, e);
        } catch (IOException e) {
            handleErrorClosingOutput(e);
        }

        return SUCCESS;
    }

    public <T> T loadAndPrintValidationErrors(InputStream inputStream, BiFunction<ConfigSchema, Boolean, T> resultHandler) throws IOException, SchemaLoaderException {
        ConvertableSchema<ConfigSchema> configSchema = SchemaLoader.loadConvertableSchemaFromYaml(inputStream);
        boolean valid = true;
        if (!configSchema.isValid()) {
            System.out.println("Found the following errors when parsing the configuration according to its version. (" + configSchema.getVersion() + ")");
            configSchema.getValidationIssues().forEach(s -> System.out.println(s));
            System.out.println();
            valid = false;
            configSchema.clearValidationIssues();
        } else {
            System.out.println("No errors found when parsing configuration according to its version. (" + configSchema.getVersion() + ")");
        }

        ConfigSchema currentSchema = configSchema.convert();
        if (!currentSchema.isValid()) {
            System.out.println("Found the following errors when converting configuration to latest version. (" + ConfigSchema.CONFIG_VERSION + ")");
            currentSchema.getValidationIssues().forEach(s -> System.out.println(s));
            System.out.println();
            valid = false;
        } else if (configSchema.getVersion() == currentSchema.getVersion()) {
            System.out.println("Configuration was already latest version (" + ConfigSchema.CONFIG_VERSION + ") so no conversion was needed.");
        } else {
            System.out.println("No errors found when converting configuration to latest version. (" + ConfigSchema.CONFIG_VERSION + ")");
        }
        return resultHandler.apply(currentSchema, valid);
    }

    public int transform(String[] args) {
        if (args.length != 3) {
            printTransformUsage();
            return ERR_INVALID_ARGS;
        }

        ConfigSchema configSchema = null;
        try (InputStream inputStream = pathInputStreamFactory.create(args[1])) {
            try {
                configSchema = transformTemplateToSchema(inputStream);
                if (!configSchema.isValid()) {
                    System.out.println(THERE_ARE_VALIDATION_ERRORS_WITH_THE_TEMPLATE_STILL_OUTPUTTING_YAML_BUT_IT_WILL_NEED_TO_BE_EDITED);
                    configSchema.getValidationIssues().forEach(System.out::println);
                    System.out.println();
                } else {
                    System.out.println("No validation errors found in converted configuration.");
                }
            } catch (JAXBException e) {
                System.out.println("Error reading template. (" + e + ")");
                System.out.println();
                printTransformUsage();
                return ERR_UNABLE_TO_READ_TEMPLATE;
            }
        } catch (FileNotFoundException e) {
            return handleErrorOpeningInput(args[1], ConfigMain::printTransformUsage, e);
        } catch (IOException e) {
            handleErrorClosingInput(e);
        }

        try (OutputStream fileOutputStream = pathOutputStreamFactory.create(args[2])) {
            try {
                SchemaSaver.saveConfigSchema(configSchema, fileOutputStream);
            } catch (IOException e) {
                return handleErrorSavingCofiguration(e);
            }
        } catch (FileNotFoundException e) {
            return handleErrorOpeningOutput(args[2], ConfigMain::printTransformUsage, e);
        } catch (IOException e) {
            handleErrorClosingOutput(e);
        }

        return SUCCESS;
    }

    protected void handleErrorClosingOutput(IOException e) {
        System.out.println("Error closing output. (" + e + ")");
        System.out.println();
    }

    protected void handleErrorClosingInput(IOException e) {
        System.out.println("Error closing input. (" + e + ")");
        System.out.println();
    }

    protected int handleErrorOpeningInput(String fileName, Runnable usagePrinter, FileNotFoundException e) {
        System.out.println("Unable to open file " + fileName + " for reading. (" + e + ")");
        System.out.println();
        usagePrinter.run();
        return ERR_UNABLE_TO_OPEN_INPUT;
    }

    protected int handleErrorOpeningOutput(String fileName, Runnable usagePrinter, FileNotFoundException e) {
        System.out.println("Unable to open file " + fileName + " for writing. (" + e + ")");
        System.out.println();
        usagePrinter.run();
        return ERR_UNABLE_TO_OPEN_OUTPUT;
    }

    protected int handleErrorLoadingConfiguration(Exception e, Runnable usagePrinter) {
        System.out.println("Unable to load configuration. (" + e + ")");
        System.out.println();
        usagePrinter.run();
        return ERR_UNABLE_TO_PARSE_CONFIG;
    }

    protected int handleErrorSavingCofiguration(IOException e) {
        System.out.println("Unable to save configuration: " + e);
        System.out.println();
        return ERR_UNABLE_TO_SAVE_CONFIG;
    }

    public int execute(String[] args) {
        if (args.length < 1 || !commandMap.containsKey(args[0].toLowerCase())) {
            printUsage();
            return ERR_INVALID_ARGS;
        }
        return commandMap.get(args[0].toLowerCase()).function.apply(args);
    }

    public Map<String, Command> createCommandMap() {
        Map<String, Command> result = new TreeMap<>();
        result.put(TRANSFORM, new Command(this::transform, "Transform template xml into MiNiFi config YAML"));
        result.put(VALIDATE, new Command(this::validate, "Validate config YAML"));
        result.put(UPGRADE, new Command(this::upgrade, "Upgrade config YAML to current version (" + ConfigSchema.CONFIG_VERSION + ")"));
        return result;
    }

    public void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("Valid commands include:");
        commandMap.forEach((s, command) -> System.out.println(s + ": " + command.description));
    }

    public class Command {
        private final Function<String[], Integer> function;
        private final String description;

        public Command(Function<String[], Integer> function, String description) {
            this.function = function;
            this.description = description;
        }
    }
}
