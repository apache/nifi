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
package org.apache.nifi.toolkit.encryptconfig

import org.apache.commons.cli.HelpFormatter
import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.apache.nifi.properties.SensitivePropertyProvider
import org.apache.nifi.util.console.TextDevices
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NiFiRegistryMode implements ToolMode {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryMode.class)

    private static final String MODE_NAME = "nifi-registry"

    CliBuilder cli

    NiFiRegistryMode() {
        super
        cli = cliBuilder()
    }

    @Override
    String getModeName() {
        return MODE_NAME
    }

    @Override
    String getModeDescription() {
        return "Protect sensitive values in NiFi Registry config files by encrypting them with a master key."
    }

    void printUsage(String message = "") {
        if (message) {
            System.out.println(message)
            System.out.println()
        }
        cli.usage()
    }

    void printUsageAndExit(String message = "", int exitStatusCode) {
        printUsage(message)
        System.exit(exitStatusCode)
    }

    @Override
    void run(String[] args) {

        try {

            def options = cli.parse(args)

            if (!options || options.h) {
                printUsageAndExit("", EncryptConfig.EXIT_STATUS_OTHER)
            }

            EncryptConfigLogger.configureLogger(options.v)

            Configuration config = new Configuration(options)
            run(config)

        } catch (Exception e) {
            logger.error("Encountered an error: ${e.getMessage()}", e)
            printUsageAndExit(e.getMessage(), EncryptConfig.EXIT_STATUS_FAILURE)
        }
    }

    void run(Configuration config) throws Exception {

        if (config.usingPassword) {
            logger.info("Using encryption key derived from password.")
        } else if (config.usingRawKeyHex) {
            logger.info("Using encryption key provided.")
        } else if (config.usingBootstrapKey) {
            logger.info("Using encryption key from input bootstrap.conf.")
        }

        logger.debug("(src)  bootstrap.conf:         ${config.inputBootstrapPath}")
        logger.debug("(dest) bootstrap.conf:         ${config.outputBootstrapPath}")
        logger.debug("(src)  nifi.properties:        ${config.inputNiFiRegistryPropertiesPath}")
        logger.debug("(dest) nifi.properties:        ${config.outputNiFiRegistryPropertiesPath}")
        logger.debug("(src)  identity-providers.xml: ${config.inputIdentityProvidersPath}")
        logger.debug("(dest) identity-providers.xml: ${config.outputIdentityProvidersPath}")
        logger.debug("(src)  authorizers.xml:        ${config.inputAuthorizersPath}")
        logger.debug("(dest) authorizers.xml:        ${config.outputAuthorizersPath}")

        Properties niFiRegistryProperties = null
        if (config.handlingNiFiRegistryProperties) {
            try {
                niFiRegistryProperties = config.propertiesEncryptor.loadFile(config.inputNiFiRegistryPropertiesPath)
                // if properties are not protected, then the call to decrypt is a no-op
                niFiRegistryProperties = config.propertiesEncryptor.decrypt(niFiRegistryProperties)
                niFiRegistryProperties = config.propertiesEncryptor.encrypt(niFiRegistryProperties)
            } catch (Exception e) {
                def msg = "Encountered error trying to load and encrypt NiFi Registry Properties in ${config.inputNiFiRegistryPropertiesPath}."
                logger.debug(msg, e)
                printUsageAndExit(msg, EncryptConfig.EXIT_STATUS_FAILURE)
            }
        }

        String identityProvidersXml = null
        if (config.handlingIdentityProviders) {
            try {
                identityProvidersXml = config.identityProvidersXmlEncryptor.loadXmlFile(config.inputIdentityProvidersPath)
                // if xml is not protected, then the call to decrypt is a no-op
                identityProvidersXml = config.identityProvidersXmlEncryptor.decrypt(identityProvidersXml)
                identityProvidersXml = config.identityProvidersXmlEncryptor.encrypt(identityProvidersXml)
            } catch (Exception e) {
                def msg = "Encountered error trying to load and encrypt Identity Providers XML in ${config.inputIdentityProvidersPath}."
                logger.debug(msg, e)
                printUsageAndExit(msg, EncryptConfig.EXIT_STATUS_FAILURE)
            }
        }

        String authorizersXml = null
        if (config.handlingAuthorizers) {
            try {
                authorizersXml = config.authorizersXmlEncryptor.loadXmlFile(config.inputAuthorizersPath)
                // if xml is not protected, then the call to decrypt is a no-op
                authorizersXml = config.authorizersXmlEncryptor.decrypt(authorizersXml)
                authorizersXml = config.authorizersXmlEncryptor.encrypt(authorizersXml)
            } catch (Exception e) {
                def msg = "Encountered error trying to load and encrypt Authorizers XML in ${config.inputAuthorizersPath}."
                logger.debug(msg, e)
                printUsageAndExit(msg, EncryptConfig.EXIT_STATUS_FAILURE)
            }
        }

        try {
            // Do this as part of a transaction?
            synchronized (this) {

                if (config.writingKeyToBootstrap) {
                    BootstrapUtil.writeKey(config.encryptionKey, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY, config.outputBootstrapPath, config.inputBootstrapPath)
                    logger.info("Updated bootstrap config file with master key: ${config.outputBootstrapPath}")
                }

                if (config.handlingNiFiRegistryProperties) {
                    config.propertiesEncryptor.write(niFiRegistryProperties, config.outputNiFiRegistryPropertiesPath, config.inputNiFiRegistryPropertiesPath)
                    logger.info("Updated NiFi Registry Properties file with protected values: ${config.outputNiFiRegistryPropertiesPath}")
                }
                if (config.handlingIdentityProviders) {
                    config.identityProvidersXmlEncryptor.writeXmlFile(identityProvidersXml, config.outputIdentityProvidersPath, config.inputIdentityProvidersPath)
                    logger.info("Updated Identity Providers XML file with protected values: ${config.outputIdentityProvidersPath}")
                }
                if (config.handlingAuthorizers) {
                    config.authorizersXmlEncryptor.writeXmlFile(authorizersXml, config.outputAuthorizersPath, config.inputAuthorizersPath)
                    logger.info("Updated Authorizers XML file with protected values: ${config.outputAuthorizersPath}")
                }
            }
        } catch (Exception e) {
            logger.debug("Encountered an error: ", e)
            throw new RuntimeException("Encountered an error writing the output files.", e)
        }
    }

    private CliBuilder cliBuilder() {

        String usage = "${EncryptConfig.class.getCanonicalName()} ${MODE_NAME} [options]"

        int formatWidth = EncryptConfig.HELP_FORMAT_WIDTH
        HelpFormatter formatter = new HelpFormatter()
        formatter.setWidth(formatWidth)
        formatter.setOptionComparator(null) // preserve order of options below in help text

        CliBuilder cli = new CliBuilder(
                usage: usage,
                header: getModeDescription(),
                width: formatWidth,
                formatter: formatter)

        cli.h(longOpt: 'help', 'Show usage information (this message)')
        cli.v(longOpt: 'verbose', 'Enables verbose mode (off by default)')

        // Options for the new password or key
        cli.p(longOpt: 'password',
                args: 1,
                argName: 'password',
                optionalArg: true,
                'Protect the files using a password-derived key. If an argument is not provided to this flag, interactive mode will be triggered to prompt the user to enter the password.')
        cli.k(longOpt: 'key',
                args: 1,
                argName: 'keyhex',
                optionalArg: true,
                'Protect the files using a raw hexadecimal key. If an argument is not provided to this flag, interactive mode will be triggered to prompt the user to enter the key.')

        // Options for the old password or key, if running the tool to migrate keys
        cli._(longOpt: 'oldPassword',
                args: 1,
                argName: 'password',
                'If the input files are already protected using a password-derived key, this specifies the old password so that the files can be unprotected before re-protecting.')
        cli._(longOpt: 'oldKey',
                args: 1,
                argName: 'keyhex',
                'If the input files are already protected using a key, this specifies the raw hexadecimal key so that the files can be unprotected before re-protecting.')

        // Options for output bootstrap.conf file
        cli.b(longOpt: 'bootstrapConf',
                args: 1,
                argName: 'file',
                'The bootstrap.conf file containing no master key or an existing master key. If a new password/key is specified and no output bootstrap.conf file is specified, then this file will be overwritten to persist the new master key.')
        cli.B(longOpt: 'outputBootstrapConf',
                args: 1,
                argName: 'file',
                'The destination bootstrap.conf file to persist master key. If specified, the input bootstrap.conf will not be modified.')

        // Options for input/output nifi-registry.properties files
        cli.r(longOpt: 'nifiRegistryProperties',
                args: 1,
                argName: 'file',
                'The nifi-registry.properties file containing unprotected config values, overwritten if no output file specified.')
        cli.R(longOpt: 'outputNifiRegistryProperties',
                args: 1,
                argName: 'file',
                'The destination nifi-registry.properties file containing protected config values.')

        // Options for input/output authorizers.xml files
        cli.a(longOpt: 'authorizersXml',
                args: 1,
                argName: 'file',
                'The authorizers.xml file containing unprotected config values, overwritten if no output file specified.')
        cli.A(longOpt: 'outputAuthorizersXml',
                args: 1,
                argName: 'file',
                'The destination authorizers.xml file containing protected config values.')

        // Options for input/output identity-providers.xml files
        cli.i(longOpt: 'identityProvidersXml',
                args: 1,
                argName: 'file',
                'The identity-providers.xml file containing unprotected config values, overwritten if no output file specified.')
        cli.I(longOpt: 'outputIdentityProvidersXml',
                args: 1,
                argName: 'file',
                'The destination identity-providers.xml file containing protected config values.')

        // TODO, add a footer with usage examples

        return cli

    }

    class Configuration {

        OptionAccessor rawOptions

        boolean usingRawKeyHex
        boolean usingPassword
        boolean usingBootstrapKey

        String encryptionKey
        String decryptionKey

        SensitivePropertyProvider encryptionProvider
        SensitivePropertyProvider decryptionProvider

        boolean writingKeyToBootstrap = false
        String inputBootstrapPath
        String outputBootstrapPath

        boolean handlingNiFiRegistryProperties = false
        String inputNiFiRegistryPropertiesPath
        String outputNiFiRegistryPropertiesPath
        NiFiRegistryPropertiesEncryptor propertiesEncryptor

        boolean handlingIdentityProviders = false
        String inputIdentityProvidersPath
        String outputIdentityProvidersPath
        NiFiRegistryIdentityProvidersXmlEncryptor identityProvidersXmlEncryptor

        boolean handlingAuthorizers = false
        String inputAuthorizersPath
        String outputAuthorizersPath
        NiFiRegistryAuthorizersXmlEncryptor authorizersXmlEncryptor

        Configuration() {
        }

        Configuration(OptionAccessor options) {
            this.rawOptions = options

            validateOptions()

            // Set input bootstrap.conf path
            inputBootstrapPath = rawOptions.b

            // Determine key for encryption (required)
            determineEncryptionKey()
            if (!encryptionKey) {
                throw new RuntimeException("Failed to configure tool, could not determine encryption key. Must provide -p, -k, or -b. If using -b, bootstrap.conf argument must already contain master key.")
            }
            encryptionProvider = new AESSensitivePropertyProvider(encryptionKey)

            // Determine key for decryption (if migrating)
            determineDecryptionKey()
            if (!decryptionKey) {
                logger.debug("No decryption key specified via options, so if any input files require decryption prior to re-encryption (i.e., migration), this tool will fail.")
            }
            decryptionProvider = decryptionKey ? new AESSensitivePropertyProvider(decryptionKey) : null

            writingKeyToBootstrap = (usingPassword || usingRawKeyHex || rawOptions.B)
            if (writingKeyToBootstrap) {
                outputBootstrapPath = rawOptions.B ? rawOptions.B : inputBootstrapPath
            }

            handlingNiFiRegistryProperties = rawOptions.r
            if (handlingNiFiRegistryProperties) {
                inputNiFiRegistryPropertiesPath = rawOptions.r
                outputNiFiRegistryPropertiesPath = rawOptions.R ? rawOptions.R : inputNiFiRegistryPropertiesPath
                propertiesEncryptor = new NiFiRegistryPropertiesEncryptor(encryptionProvider, decryptionProvider)
            }

            handlingIdentityProviders = rawOptions.i
            if (handlingIdentityProviders) {
                inputIdentityProvidersPath = rawOptions.i
                outputIdentityProvidersPath = rawOptions.I ? rawOptions.I : inputIdentityProvidersPath
                identityProvidersXmlEncryptor = new NiFiRegistryIdentityProvidersXmlEncryptor(encryptionProvider, decryptionProvider)
            }

            handlingAuthorizers = rawOptions.a
            if (handlingAuthorizers) {
                inputAuthorizersPath = rawOptions.a
                outputAuthorizersPath = rawOptions.A ? rawOptions.A : inputAuthorizersPath
                authorizersXmlEncryptor = new NiFiRegistryAuthorizersXmlEncryptor(encryptionProvider, decryptionProvider)
            }

        }

        private void validateOptions() {

            String validationFailedMessage = null

            if (!rawOptions.b) {
                validationFailedMessage = "-b flag for bootstrap.conf is required."
                if (rawOptions.B) {
                    validationFailedMessage += " Input bootsrap.conf will be used as template for output bootstrap.conf"
                } else if (rawOptions.p || rawOptions.k) {
                    validationFailedMessage = " Encryption key will be persisted to bootstrap.conf"
                }
            }

            if (validationFailedMessage) {
                throw new RuntimeException("Invalid options: " + validationFailedMessage)
            }

        }

        private void determineEncryptionKey() {
            if (rawOptions.p || rawOptions.k) {
                String password = null
                String keyHex = null
                if (rawOptions.p) {
                    logger.debug("Attempting to generate key from password.")
                    usingPassword = true
                    password = rawOptions.p
                } else {
                    usingRawKeyHex = true
                    keyHex = rawOptions.k
                }
                encryptionKey = ToolUtilities.determineKey(TextDevices.defaultTextDevice(), keyHex, password, usingPassword)
            } else if (rawOptions.b) {
                logger.debug("Attempting to read master key from input bootstrap.conf file.")
                usingBootstrapKey = true
                encryptionKey = BootstrapUtil.extractKeyFromInputFile(inputBootstrapPath, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY)
                if (!encryptionKey) {
                    logger.warn("-b specified without -p or -k, but the input bootstrap.conf file did not contain a master key.")
                }
            }
        }

        private String determineDecryptionKey() {
            if (rawOptions.oldPassword) {
                logger.debug("Attempting to generate decryption key (for migration) from old password.")
                encryptionKey = ToolUtilities.determineKey(TextDevices.defaultTextDevice(), null, rawOptions.oldPassword, true)
            } else if (rawOptions.oldKey) {
                decryptionKey = rawOptions.oldKey
            }
        }

    }

}
