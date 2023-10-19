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
package org.apache.nifi.toolkit.config.command;

import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.toolkit.config.command.converter.ProtectionSchemeTypeConverter;
import org.apache.nifi.toolkit.config.crypto.DerivedKeyGenerator;
import org.apache.nifi.toolkit.config.crypto.StandardDerivedKeyGenerator;
import org.apache.nifi.toolkit.config.transformer.BootstrapConfigurationFileTransformer;
import org.apache.nifi.toolkit.config.transformer.FileTransformer;
import org.apache.nifi.toolkit.config.transformer.XmlFileTransformer;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Supplier;

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
                "encrypt-config supports protection of sensitive values in Apache NiFi and Apache NiFi Registry",
                "",
                "  --nifiRegistry            Targets Apache NiFi Registry configuration files",
                ""
        }
)
public class SharedEncryptConfig {

    private static final String DEFAULT_PROTECTION_SCHEME = "AES_GCM";

    private static final String WORKING_FILE_NAME_FORMAT = "%s.%d.working";

    private static final DerivedKeyGenerator derivedKeyGenerator = new StandardDerivedKeyGenerator();

    @Option(
            names = {"-v", "--verbose"},
            description = "Enable verbose logging for debugging"
    )
    boolean verboseModeEnabled;

    @Option(
            names = {"-a", "--authorizers"},
            description = "Path to file containing Authorizers [authorizers.xml] configuration that will be updated unless the output argument is provided"
    )
    Path authorizersPath;

    @Option(
            names = {"-u", "--outputAuthorizers"},
            description = "Path to output file for Authorizers [authorizers.xml] with property protection applied"
    )
    Path outputAuthorizersPath;

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
            names = {"-S", "--protectionScheme"},
            description = "Protection Scheme for values stored in Application Properties and other configuration files. Default is ${DEFAULT-VALUE}",
            defaultValue = DEFAULT_PROTECTION_SCHEME,
            converter = ProtectionSchemeTypeConverter.class
    )
    ProtectionScheme protectionScheme;

    @Option(
            names = {"-H", "--oldProtectionScheme"},
            description = "Previous Protection Scheme for values stored in Application Properties required for migration processing. Default is ${DEFAULT-VALUE}",
            defaultValue = DEFAULT_PROTECTION_SCHEME,
            converter = ProtectionSchemeTypeConverter.class
    )
    ProtectionScheme oldProtectionScheme;

    @Option(
            names = {"-k", "--key"},
            description = "Bootstrap hexadecimal root key for encrypting property values in Application Properties and other configuration files",
            arity = "0..1",
            interactive = true
    )
    String key;

    @Option(
            names = {"-e", "--oldKey"},
            description = "Previous Bootstrap hexadecimal root key used during migration for encrypting property values in Application Properties",
            arity = "0..1",
            interactive = true
    )
    String oldKey;

    @Option(
            names = {"-p", "--password"},
            description = "Bootstrap password from which to derive the root key used to encrypt the sensitive properties in Application Properties",
            arity = "0..1",
            interactive = true
    )
    String password;

    @Option(
            names = {"-w", "--oldPassword"},
            description = "Previous Bootstrap password from which to derive the root key during migration for decrypting sensitive properties in Application Properties",
            arity = "0..1",
            interactive = true
    )
    String oldPassword;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Process bootstrap.conf writing new Root Key to specified Root Key Property when bootstrap.conf is specified
     *
     * @param rootKeyProperty Root Key Property in bootstrap.conf to be updated
     */
    protected void processBootstrapConf(final String rootKeyProperty) {
        if (bootstrapConfPath == null) {
            if (verboseModeEnabled) {
                logger.info("Bootstrap Configuration [bootstrap.conf] not specified");
            }
        } else {
            final String rootKey = getRootKey();
            if (rootKey == null) {
                logger.info("Bootstrap Root Key or Root Password not specified");
            } else {
                logger.info("Started processing Bootstrap Configuration [{}]", bootstrapConfPath);

                final FileTransformer fileTransformer = new BootstrapConfigurationFileTransformer(rootKeyProperty, rootKey);
                runFileTransformer(fileTransformer, bootstrapConfPath, outputBootstrapConf);

                logger.info("Completed processing Bootstrap Configuration [{}]", bootstrapConfPath);
            }
        }
    }

    /**
     * Process authorizers.xml decrypting sensitive values when required and encrypting sensitive values with new settings
     */
    protected void processAuthorizers() {
        if (authorizersPath == null) {
            if (verboseModeEnabled) {
                logger.info("Authorizers not specified");
            }
        } else {
            logger.info("Started processing Authorizers [{}]", authorizersPath);

            final SensitivePropertyProvider inputSensitivePropertyProvider = getInputSensitivePropertyProvider();
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getSensitivePropertyProviderFactory();
            final FileTransformer fileTransformer = new XmlFileTransformer(inputSensitivePropertyProvider, sensitivePropertyProviderFactory, protectionScheme);
            runFileTransformer(fileTransformer, authorizersPath, outputAuthorizersPath);

            logger.info("Completed processing Authorizers [{}]", authorizersPath);
        }
    }

    /**
     * Run File Transformer using working path based on output path
     *
     * @param fileTransformer File Transformer to be invoked
     * @param inputPath Input path of file to be transformed
     * @param outputPath Output path for transformed file that defaults to the input path when not specified
     */
    protected void runFileTransformer(final FileTransformer fileTransformer, final Path inputPath, final Path outputPath) {
        final Path configuredOutputPath = outputPath == null ? inputPath : outputPath;
        final Path workingPath = getWorkingPath(configuredOutputPath);
        try {
            fileTransformer.transform(inputPath, workingPath);
            Files.move(workingPath, configuredOutputPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException e) {
            final String message = String.format("Processing Configuration [%s] failed", inputPath);
            throw new UncheckedIOException(message, e);
        }
    }

    /**
     * Get Sensitive Property Provider Factory using provided Root Key or Password for derived Root Key
     *
     * @return Sensitive Property Provider Factory
     */
    protected SensitivePropertyProviderFactory getSensitivePropertyProviderFactory() {
        return StandardSensitivePropertyProviderFactory.withKeyAndBootstrapSupplier(getRootKey(), getBootstrapPropertiesSupplier());
    }

    /**
     * Get Input Sensitive Property Provider for decrypting previous values
     *
     * @return Input Sensitive Property Provider
     */
    protected SensitivePropertyProvider getInputSensitivePropertyProvider() {
        final SensitivePropertyProvider inputSensitivePropertyProvider;
        if (oldPassword == null && oldKey == null) {
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getSensitivePropertyProviderFactory();
            inputSensitivePropertyProvider = sensitivePropertyProviderFactory.getProvider(protectionScheme);
        } else {
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getInputSensitivePropertyProviderFactory();
            inputSensitivePropertyProvider = sensitivePropertyProviderFactory.getProvider(oldProtectionScheme);
        }
        return inputSensitivePropertyProvider;
    }

    /**
     * Get Bootstrap Root Key provided as hexadecimal encoded key or derived from password
     *
     * @return Root Key or null when neither Root Key nor Password specified
     */
    protected String getRootKey() {
        final String rootKey;

        if (key == null) {
            if (password == null) {
                if (verboseModeEnabled) {
                    logger.info("Neither Bootstrap Root Key nor Bootstrap Password specified");
                }
                rootKey = null;
            } else {
                if (verboseModeEnabled) {
                    logger.info("Bootstrap Root Key Derivation started");
                }
                rootKey = derivedKeyGenerator.getDerivedKeyEncoded(password.toCharArray());
            }
        } else {
            rootKey = key;
        }

        return rootKey;
    }

    /**
     * Get Previous Bootstrap Root Key provided as hexadecimal encoded key or derived from password for decrypting previous values
     *
     * @return Root Key or null when neither Root Key nor Password specified
     */
    protected String getInputRootKey() {
        final String rootKey;

        if (oldKey == null) {
            if (oldPassword == null) {
                logger.info("Neither Migration Bootstrap Root Key not Migration Bootstrap Password specified");
                rootKey = null;
            } else {
                if (verboseModeEnabled) {
                    logger.info("Migration Bootstrap Root Key Derivation started");
                }
                rootKey = derivedKeyGenerator.getDerivedKeyEncoded(oldPassword.toCharArray());
            }
        } else {
            rootKey = oldKey;
        }

        return rootKey;
    }

    private Path getWorkingPath(final Path resourcePath) {
        final Path fileName = resourcePath.getFileName();
        final String workingFileName = String.format(WORKING_FILE_NAME_FORMAT, fileName, System.currentTimeMillis());
        return resourcePath.resolveSibling(workingFileName);
    }

    private SensitivePropertyProviderFactory getInputSensitivePropertyProviderFactory() {
        return StandardSensitivePropertyProviderFactory.withKeyAndBootstrapSupplier(getInputRootKey(), getBootstrapPropertiesSupplier());
    }

    private Supplier<BootstrapProperties> getBootstrapPropertiesSupplier() {
        return () -> {
            if (bootstrapConfPath == null) {
                return BootstrapProperties.EMPTY;
            }

            try {
                return NiFiBootstrapUtils.loadBootstrapProperties(bootstrapConfPath.toString());
            } catch (final IOException e) {
                logger.warn("Loading Bootstrap Configuration failed [{}]", bootstrapConfPath, e);
                return BootstrapProperties.EMPTY;
            }
        };
    }
}
