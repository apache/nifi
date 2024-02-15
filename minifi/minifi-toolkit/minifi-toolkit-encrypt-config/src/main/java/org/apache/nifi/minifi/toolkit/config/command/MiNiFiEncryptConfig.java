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

package org.apache.nifi.minifi.toolkit.config.command;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.isAnyBlank;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_ALGORITHM;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_KEY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.minifi.commons.service.FlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.FlowSerDeService;
import org.apache.nifi.minifi.commons.service.StandardFlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.StandardFlowSerDeService;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.apache.nifi.minifi.properties.BootstrapPropertiesLoader;
import org.apache.nifi.minifi.properties.ProtectedBootstrapProperties;
import org.apache.nifi.minifi.toolkit.config.transformer.ApplicationPropertiesFileTransformer;
import org.apache.nifi.minifi.toolkit.config.transformer.BootstrapConfigurationFileTransformer;
import org.apache.nifi.minifi.toolkit.config.transformer.FileTransformer;
import org.apache.nifi.properties.AesGcmSensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Encrypt Configuration for MiNiFi
 */
@Command(
    name = "encrypt-config",
    sortOptions = false,
    mixinStandardHelpOptions = true,
    usageHelpWidth = 160,
    separator = " ",
    version = {
        "Java ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})"
    },
    descriptionHeading = "Description: ",
    description = {
        "encrypt-config supports protection of sensitive values in Apache MiNiFi"
    }
)
public class MiNiFiEncryptConfig implements Runnable {

    static final String BOOTSTRAP_ROOT_KEY_PROPERTY = "minifi.bootstrap.sensitive.key";

    private static final String WORKING_FILE_NAME_FORMAT = "%s.%d.working";
    private static final int KEY_LENGTH = 32;

    @Option(
        names = {"-b", "--bootstrapConf"},
        description = "Path to file containing Bootstrap Configuration [bootstrap.conf] for optional root key and property protection scheme settings"
    )
    Path bootstrapConfPath;

    @Option(
        names = {"-B", "--outputBootstrapConf"},
        description = "Path to output file for Bootstrap Configuration [bootstrap.conf] with root key configured"
    )
    Path outputBootstrapConf;

    @Option(
        names = {"-x", "--encryptRawFlowJsonOnly"},
        description = "Process Raw Flow Configuration [flow.json.raw] sensitive property values without modifying other configuration files"
    )
    boolean flowConfigurationRequested;

    @Option(
        names = {"-f", "--rawFlowJson"},
        description = "Path to file containing Raw Flow Configuration [flow.json.raw] that will be updated unless the output argument is provided"
    )
    Path flowConfigurationPath;

    @Option(
        names = {"-g", "--outputRawFlowJson"},
        description = "Path to output file for Raw Flow Configuration [flow.json.raw] with property protection applied"
    )
    Path outputFlowConfigurationPath;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run() {
        BootstrapProperties unprotectedProperties = BootstrapPropertiesLoader.load(bootstrapConfPath.toFile());
        processBootstrapConf(unprotectedProperties);
        processFlowConfiguration(unprotectedProperties);
    }

    /**
     * Process bootstrap.conf writing new Root Key to specified Root Key Property when bootstrap.conf is specified
     */
    protected void processBootstrapConf(BootstrapProperties unprotectedProperties) {
        if (flowConfigurationRequested) {
            logger.info("Bootstrap Configuration [bootstrap.conf] not modified based on provided arguments");
            return;
        }

        logger.info("Started processing Bootstrap Configuration [{}]", bootstrapConfPath);

        String newRootKey = getRootKey();

        Set<String> sensitivePropertyNames = new HashSet<>((new ProtectedBootstrapProperties(unprotectedProperties)).getSensitivePropertyKeys());
        FileTransformer fileTransformer2 = new ApplicationPropertiesFileTransformer(unprotectedProperties, getInputSensitivePropertyProvider(newRootKey), sensitivePropertyNames);
        runFileTransformer(fileTransformer2, bootstrapConfPath, outputBootstrapConf);

        FileTransformer fileTransformer = new BootstrapConfigurationFileTransformer(BOOTSTRAP_ROOT_KEY_PROPERTY, newRootKey);
        runFileTransformer(fileTransformer, ofNullable(outputBootstrapConf).orElse(bootstrapConfPath), outputBootstrapConf);
        logger.info("Completed processing Bootstrap Configuration [{}]", bootstrapConfPath);
    }

    private String getRootKey() {
        SecureRandom secureRandom = new SecureRandom();
        byte[] sensitivePropertiesKeyBinary = new byte[KEY_LENGTH];
        secureRandom.nextBytes(sensitivePropertiesKeyBinary);
        return HexFormat.of().formatHex(sensitivePropertiesKeyBinary);
    }

    /**
     * Run File Transformer using working path based on output path
     *
     * @param fileTransformer File Transformer to be invoked
     * @param inputPath       Input path of file to be transformed
     * @param outputPath      Output path for transformed file that defaults to the input path when not specified
     */
    protected void runFileTransformer(FileTransformer fileTransformer, Path inputPath, Path outputPath) {
        Path configuredOutputPath = outputPath == null ? inputPath : outputPath;
        Path workingPath = getWorkingPath(configuredOutputPath);
        try {
            fileTransformer.transform(inputPath, workingPath);
            Files.move(workingPath, configuredOutputPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            String message = String.format("Processing Configuration [%s] failed", inputPath);
            throw new UncheckedIOException(message, e);
        }
    }


    /**
     * Get Input Sensitive Property Provider for decrypting previous values
     *
     * @return Input Sensitive Property Provider
     */
    protected SensitivePropertyProvider getInputSensitivePropertyProvider(String keyHex) {
        return new AesGcmSensitivePropertyProvider(keyHex);
    }

    private Path getWorkingPath(Path resourcePath) {
        Path fileName = resourcePath.getFileName();
        String workingFileName = String.format(WORKING_FILE_NAME_FORMAT, fileName, System.currentTimeMillis());
        return resourcePath.resolveSibling(workingFileName);
    }

    private void processFlowConfiguration(BootstrapProperties unprotectedProperties) {
        if (flowConfigurationPath == null) {
            logger.info("Flow Configuration not specified");
            return;
        }
        String sensitivePropertiesKey = unprotectedProperties.getProperty(NIFI_MINIFI_SENSITIVE_PROPS_KEY.getKey());
        String sensitivePropertiesAlgorithm = unprotectedProperties.getProperty(NIFI_MINIFI_SENSITIVE_PROPS_ALGORITHM.getKey());
        if (isAnyBlank(sensitivePropertiesKey, sensitivePropertiesAlgorithm)) {
            logger.info("Sensitive Properties Key or Sensitive Properties Algorithm is not provided");
            return;
        }

        logger.info("Started processing Flow Configuration [{}]", flowConfigurationPath);

        byte[] flowAsBytes;
        try {
            flowAsBytes = readAllBytes(flowConfigurationPath);
        } catch (IOException e) {
            logger.error("Unable to load Flow Configuration [{}]", flowConfigurationPath);
            return;
        }

        FlowSerDeService flowSerDeService = StandardFlowSerDeService.defaultInstance();
        FlowPropertyEncryptor flowPropertyEncryptor = new StandardFlowPropertyEncryptor(
            new PropertyEncryptorBuilder(sensitivePropertiesKey).setAlgorithm(sensitivePropertiesAlgorithm).build(), null);

        VersionedDataflow flow = flowSerDeService.deserialize(flowAsBytes);
        VersionedDataflow encryptedFlow = flowPropertyEncryptor.encryptSensitiveProperties(flow);
        byte[] encryptedFlowAsBytes = flowSerDeService.serialize(encryptedFlow);

        Path targetPath = ofNullable(outputFlowConfigurationPath).orElse(flowConfigurationPath);
        try {
            write(targetPath, encryptedFlowAsBytes);
        } catch (IOException e) {
            logger.error("Unable to write Flow Configuration [{}]", targetPath);
            return;
        }

        logger.info("Completed processing Flow Configuration [{}]", flowConfigurationPath);
    }
}
