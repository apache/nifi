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

import groovy.cli.commons.CliBuilder
import groovy.cli.commons.OptionAccessor
import org.apache.commons.cli.HelpFormatter
import org.apache.nifi.properties.ConfigEncryptionTool
import org.apache.nifi.properties.PropertyProtectionScheme
import org.apache.nifi.properties.SensitivePropertyProvider
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory
import org.apache.nifi.toolkit.encryptconfig.util.BootstrapUtil
import org.apache.nifi.toolkit.encryptconfig.util.PropertiesEncryptor
import org.apache.nifi.toolkit.encryptconfig.util.ToolUtilities
import org.apache.nifi.toolkit.encryptconfig.util.XmlEncryptor
import org.apache.nifi.util.console.TextDevices
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DecryptMode implements ToolMode {

    private static final Logger logger = LoggerFactory.getLogger(DecryptMode.class)

    static enum FileType {
        properties,
        xml
    }

    CliBuilder cli
    boolean verboseEnabled

    DecryptMode() {
        cli = cliBuilder()
        verboseEnabled = false
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
                printUsageAndExit("", EncryptConfigMain.EXIT_STATUS_OTHER)
            }

            if (options.v) {
                verboseEnabled = true
            }
            EncryptConfigLogger.configureLogger(verboseEnabled)

            DecryptConfiguration config = new DecryptConfiguration(options)

            run(config)

        } catch (Exception e) {
            if (verboseEnabled) {
                logger.error("Encountered an error: ${e.getMessage()}", e)
            }
            printUsageAndExit(e.getMessage(), EncryptConfigMain.EXIT_STATUS_FAILURE)
        }
    }

    void run(DecryptConfiguration config) throws Exception {

        if (!config.fileType) {

            // Try to load the input file to auto-detect the file type
            boolean isPropertiesFile = PropertiesEncryptor.supportsFile(config.inputFilePath)

            boolean isXmlFile = XmlEncryptor.supportsFile(config.inputFilePath)

            if (ToolUtilities.isExactlyOneTrue(isPropertiesFile, isXmlFile)) {
                if (isPropertiesFile) {
                    config.fileType = FileType.properties
                    logger.debug("Auto-detection of input file type determined the type to be: ${FileType.properties}")
                }
                if (isXmlFile) {
                    config.fileType = FileType.xml
                    logger.debug("Auto-detection of input file type determined the type to be: ${FileType.xml}")
                }
            }

            // Could we successfully auto-detect?
            if (!config.fileType) {
                throw new RuntimeException("Auto-detection of input file type failed. Please re-run the tool specifying the file type with the -t/--fileType flag.")
            }
        }

        String decryptedSerializedContent = null
        switch (config.fileType) {

            case FileType.properties:
                PropertiesEncryptor propertiesEncryptor = new PropertiesEncryptor(null, config.decryptionProvider)
                Properties properties = propertiesEncryptor.loadFile(config.inputFilePath)
                properties = propertiesEncryptor.decrypt(properties)
                decryptedSerializedContent = propertiesEncryptor.serializePropertiesAndPreserveFormatIfPossible(properties, config.inputFilePath)
                break

            case FileType.xml:
                XmlEncryptor xmlEncryptor = new XmlEncryptor(null, config.decryptionProvider) {
                    @Override
                    List<String> serializeXmlContentAndPreserveFormat(String updatedXmlContent, String originalXmlContent) {
                        // For decrypting unknown, generic XML, this tool will not support preserving the format
                        return updatedXmlContent.split("\n")
                    }
                }

                String xmlContent = xmlEncryptor.loadXmlFile(config.inputFilePath)
                xmlContent = xmlEncryptor.decrypt(xmlContent)
                decryptedSerializedContent = xmlEncryptor.serializeXmlContentAndPreserveFormatIfPossible(xmlContent, config.inputFilePath)
                break

            default:
                throw new RuntimeException("Unsupported file type '${config.fileType}'")
        }

        if (!decryptedSerializedContent) {
            throw new RuntimeException("Failed to load and decrypt input file.")
        }

        if (config.outputToFile) {
            try {
                File outputFile = new File(config.outputFilePath)
                if (ToolUtilities.isSafeToWrite(outputFile)) {
                    outputFile.text = decryptedSerializedContent
                    logger.info("Wrote decrypted file contents to '${config.outputFilePath}'")
                }
            } catch (IOException e) {
                throw new RuntimeException("Encountered an exception writing the decrypted content to '${config.outputFilePath}': ${e.getMessage()}", e)
            }
        } else {
            System.out.println(decryptedSerializedContent)
        }

    }

    private CliBuilder cliBuilder() {

        String usage = "${EncryptConfigMain.class.getCanonicalName()} decrypt [options] file"

        int formatWidth = EncryptConfigMain.HELP_FORMAT_WIDTH
        HelpFormatter formatter = new HelpFormatter()
        formatter.setWidth(formatWidth)
        formatter.setOptionComparator(null) // preserve order of options below in help text

        CliBuilder cli = new CliBuilder(
                usage: usage,
                width: formatWidth,
                formatter: formatter,
                stopAtNonOption: false)

        cli.h(longOpt: 'help', 'Show usage information (this message)')
        cli.v(longOpt: 'verbose', 'Enables verbose mode (off by default)')

        // Options for the password or key or bootstrap.conf
        cli.p(longOpt: 'password',
                args: 1,
                argName: 'password',
                optionalArg: true,
                'Use a password to derive the key to decrypt the input file. If an argument is not provided to this flag, interactive mode will be triggered to prompt the user to enter the password.')
        cli.k(longOpt: 'key',
                args: 1,
                argName: 'keyhex',
                optionalArg: true,
                'Use a raw hexadecimal key to decrypt the input file. If an argument is not provided to this flag, interactive mode will be triggered to prompt the user to enter the key.')
        cli.b(longOpt: 'bootstrapConf',
                args: 1,
                argName: 'file',
                'Use a bootstrap.conf file containing the root key to decrypt the input file (as an alternative to -p or -k)')

        cli.o(longOpt: 'output',
                args: 1,
                argName: 'file',
                'Specify an output file. If omitted, Standard Out is used. Output file can be set to the input file to decrypt the file in-place.')

        return cli

    }

    static class DecryptConfiguration implements Configuration {

        OptionAccessor rawOptions

        Configuration.KeySource keySource
        PropertyProtectionScheme protectionScheme = ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME
        String key
        SensitivePropertyProvider decryptionProvider
        String inputBootstrapPath

        FileType fileType

        String inputFilePath

        boolean outputToFile = false
        String outputFilePath

        DecryptConfiguration() {
        }

        DecryptConfiguration(OptionAccessor options) {
            this.rawOptions = options

            validateOptions()
            determineInputFileFromRemainingArgs()

            determineProtectionScheme()
            determineBootstrapProperties()
            if (protectionScheme.requiresSecretKey()) {
                determineKey()
                if (!key) {
                    throw new RuntimeException("Failed to configure tool, could not determine key.")
                }
            }
            decryptionProvider = StandardSensitivePropertyProviderFactory
                    .withKeyAndBootstrapSupplier(key, ConfigEncryptionTool.getBootstrapSupplier(inputBootstrapPath))
                    .getProvider(protectionScheme)

            if (rawOptions.t) {
                fileType = FileType.valueOf(rawOptions.t)
            }

            if (rawOptions.o) {
                outputToFile = true
                outputFilePath = rawOptions.o
            }
        }

        private void determineBootstrapProperties() {
            if (rawOptions.b) {
                inputBootstrapPath = rawOptions.b
            }
        }

        private void validateOptions() {

            String validationFailedMessage = null

            if (!rawOptions.b && !rawOptions.p && !rawOptions.k) {
                validationFailedMessage = "-p, -k, or -b is required in order to determine the root key to use for decryption."
            }

            if (validationFailedMessage) {
                throw new RuntimeException("Invalid options: " + validationFailedMessage)
            }

        }

        private void determineInputFileFromRemainingArgs() {
            String[] remainingArgs = this.rawOptions.getArgs()
            if (remainingArgs.length == 0) {
                throw new RuntimeException("Missing argument: Input file must be provided.")
            } else if (remainingArgs.length > 1) {
                throw new RuntimeException("Too many arguments: Please specify exactly one input file in addition to the options.")
            }
            this.inputFilePath = remainingArgs[0]
        }

        private void determineProtectionScheme() {

            if (rawOptions.S) {
                protectionScheme = PropertyProtectionScheme.valueOf(rawOptions.S)
            }
        }

        private void determineKey() {

            boolean usingPassword = false
            boolean usingRawKeyHex = false
            boolean usingBootstrapKey = false

            if (rawOptions.p) {
                usingPassword = true
            }
            if (rawOptions.k) {
                usingRawKeyHex = true
            }
            if (rawOptions.b) {
                usingBootstrapKey = true
            }

            if (!ToolUtilities.isExactlyOneTrue(usingPassword, usingRawKeyHex, usingBootstrapKey)) {
                throw new RuntimeException("Invalid options: Only one of [-p, -k, -b] is allowed for specifying the decryption password/key.")
            }

            if (usingPassword || usingRawKeyHex) {
                String password = null
                String keyHex = null
                if (usingPassword) {
                    logger.debug("Using password to derive root key for decryption")
                    password = rawOptions.getOptionValue("p")
                    keySource = Configuration.KeySource.PASSWORD
                } else {
                    logger.debug("Using raw key hex as root key for decryption")
                    keyHex = rawOptions.getOptionValue("k")
                    keySource = Configuration.KeySource.KEY_HEX
                }
                key = ToolUtilities.determineKey(TextDevices.defaultTextDevice(), keyHex, password, usingPassword)
            } else if (usingBootstrapKey) {
                logger.debug("Looking in bootstrap conf file ${inputBootstrapPath} for root key for decryption.")

                // first, try to treat the bootstrap file as a NiFi bootstrap.conf
                logger.debug("Checking expected NiFi bootstrap.conf format")
                key = BootstrapUtil.extractKeyFromBootstrapFile(inputBootstrapPath, BootstrapUtil.NIFI_BOOTSTRAP_KEY_PROPERTY)

                // if the key is still null, try again, this time treating the bootstrap file as a NiFi Registry bootstrap.conf
                if (!key) {
                    logger.debug("Checking expected NiFi Registry bootstrap.conf format")
                    key = BootstrapUtil.extractKeyFromBootstrapFile(inputBootstrapPath, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY)
                }

                // check we have found the key after trying all bootstrap formats
                if (key) {
                    logger.debug("Root key found in ${inputBootstrapPath}. This key will be used for decryption operations.")
                    keySource = Configuration.KeySource.BOOTSTRAP_FILE
                } else {
                    logger.warn("Bootstrap Conf flag present, but root key could not be found in ${inputBootstrapPath}.")
                }
            }
        }

    }
}
