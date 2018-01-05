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

import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.apache.nifi.toolkit.encryptconfig.util.BootstrapUtil
import org.apache.nifi.toolkit.encryptconfig.util.ToolUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * A special DecryptMode that can run using NiFiRegistry CLI Options
 */
class NiFiRegistryDecryptMode extends DecryptMode {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryDecryptMode.class)

    CliBuilder cli

    NiFiRegistryDecryptMode() {
        cli = NiFiRegistryMode.cliBuilder()
    }

    @Override
    void run(String[] args) {
        logger.warn("The decryption capability of this tool is still considered experimental. The results should be manually verified.")
        try {

            def options = cli.parse(args)

            if (!options || options.h) {
                EncryptConfigMain.printUsageAndExit("", EncryptConfigMain.EXIT_STATUS_OTHER)
            }

            EncryptConfigLogger.configureLogger(options.v)

            DecryptConfiguration config = new DecryptConfiguration()

            /* Invalid fields when used with --decrypt: */
            def invalidDecryptOptions = ["i", "a"]
            def presentInvalidOptions = Arrays.stream(options.getInner().getOptions()).findAll {
                invalidDecryptOptions.contains(it.getOpt())
            }
            if (presentInvalidOptions.size() > 0) {
                throw new RuntimeException("Invalid options: ${EncryptConfigMain.DECRYPT_OPT} cannot be used with [${presentInvalidOptions.join(", ")}]. It should only be used with [-r].")
            }

            /* Required fields when using --decrypt */
            // registryPropertiesFile (-r)
            if (!options.r) {
                throw new RuntimeException("Invalid options: Input nifiRegistryProperties (-r) is required when using --decrypt")
            }
            config.inputFilePath = options.r
            config.fileType = FileType.properties  // disables auto-detection, which is still experimental

            // one of [--oldPassword, --oldKey] or [-p, -k, -b <file]
            String keyHex = null
            String password = null
            config.keySource = null
            if (options.oldPassword) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [--oldPassword, --oldKey, -b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.PASSWORD
                keyHex = options.getInner().getOptionValue("oldPassword")
            }
            if (options.oldKey) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [--oldPassword, --oldKey, -b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.KEY_HEX
                keyHex = options.getInner().getOptionValue("oldKey")
            }
            if (options.p) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [--oldPassword, --oldKey, -b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.PASSWORD
                password = options.getInner().getOptionValue("p")
            }
            if (options.k) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [--oldPassword, --oldKey, -b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.KEY_HEX
                keyHex = options.getInner().getOptionValue("k")
            }

            if (config.keySource) {
                config.key = ToolUtilities.determineKey(keyHex, password, Configuration.KeySource.PASSWORD.equals(config.keySource))
            }

            if (options.b) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [--oldPassword, --oldKey, -b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.BOOTSTRAP_FILE
                config.inputBootstrapPath = options.b

                logger.debug("Checking expected NiFi Registry bootstrap.conf format")
                config.key = BootstrapUtil.extractKeyFromBootstrapFile(config.inputBootstrapPath, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY)

                // check we have found the key
                if (config.key) {
                    logger.debug("Master key found in ${config.inputBootstrapPath}. This key will be used for decryption operations.")
                } else {
                    logger.warn("Bootstrap Conf flag present, but master key could not be found in ${config.inputBootstrapPath}.")
                }
            }

            config.decryptionProvider = new AESSensitivePropertyProvider(config.key)

            /* Optional fields when using --decrypt */
            // -R outputRegistryPropertiesFile (-R)
            if (options.R) {
                config.outputToFile = true
                config.outputFilePath = options.R
            }

            run(config)

        } catch (Exception e) {
            logger.error("Encountered an error: ${e.getMessage()}")
            logger.debug("", e)  // only print stack trace when verbose is enabled
            EncryptConfigMain.printUsageAndExit(EncryptConfigMain.EXIT_STATUS_FAILURE)
        }
    }

}
