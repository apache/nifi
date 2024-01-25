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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Set;
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
 * Shared Encrypt Configuration for NiFi and NiFi Registry
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
public class MiNiFiEncryptConfig implements Runnable{

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

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run() {
        processBootstrapConf();
    }

    /**
     * Process bootstrap.conf writing new Root Key to specified Root Key Property when bootstrap.conf is specified
     *
     */
    protected void processBootstrapConf() {
        BootstrapProperties unprotectedProperties = BootstrapPropertiesLoader.load(bootstrapConfPath.toFile());

        logger.info("Started processing Bootstrap Configuration [{}]", bootstrapConfPath);

        String newRootKey = getRootKey();

        Set<String> sensitivePropertyNames = new HashSet<>((new ProtectedBootstrapProperties(unprotectedProperties)).getSensitivePropertyKeys());
        FileTransformer fileTransformer2 = new ApplicationPropertiesFileTransformer(unprotectedProperties, getInputSensitivePropertyProvider(newRootKey), sensitivePropertyNames);
        runFileTransformer(fileTransformer2, bootstrapConfPath, outputBootstrapConf);

        FileTransformer fileTransformer = new BootstrapConfigurationFileTransformer(BOOTSTRAP_ROOT_KEY_PROPERTY, newRootKey);
        runFileTransformer(fileTransformer, Optional.ofNullable(outputBootstrapConf).orElse(bootstrapConfPath), outputBootstrapConf);
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
     * @param inputPath Input path of file to be transformed
     * @param outputPath Output path for transformed file that defaults to the input path when not specified
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
}
