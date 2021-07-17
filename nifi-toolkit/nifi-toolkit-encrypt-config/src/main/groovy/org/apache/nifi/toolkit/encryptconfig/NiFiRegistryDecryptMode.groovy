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
import org.apache.nifi.properties.PropertyProtectionScheme
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory
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
    boolean verboseEnabled

    NiFiRegistryDecryptMode() {
        cli = NiFiRegistryMode.cliBuilder()
        verboseEnabled = false
    }

    @Override
    void run(String[] args) {
        try {

            def options = cli.parse(args)

            if (!options || options.h) {
                EncryptConfigMain.printUsageAndExit("", EncryptConfigMain.EXIT_STATUS_OTHER)
            }

            if (options.v) {
                verboseEnabled = true
            }
            EncryptConfigLogger.configureLogger(verboseEnabled)

            DecryptConfiguration config = new DecryptConfiguration()

            /* Invalid fields when used with --decrypt: */
            def invalidDecryptOptions = ["R", "i", "I", "a", "A", "oldPassword", "oldKey"]
            def presentInvalidOptions = Arrays.stream(options.getOptions()).findAll {
                invalidDecryptOptions.contains(it.getOpt())
            }
            if (presentInvalidOptions.size() > 0) {
                throw new RuntimeException("Invalid options: ${EncryptConfigMain.DECRYPT_FLAG} cannot be used with [${presentInvalidOptions.join(", ")}]. It should only be used with -r and one of [-p, -k, -b].")
            }

            /* Required fields when using --decrypt */
            // registryPropertiesFile (-r)
            if (!options.r) {
                throw new RuntimeException("Invalid options: Input nifiRegistryProperties (-r) is required when using --decrypt")
            }
            config.inputFilePath = options.r
            config.fileType = FileType.properties  // disables auto-detection, which is still experimental

            if (options.S) {
                config.protectionScheme = PropertyProtectionScheme.valueOf((String) options.S)
            }

            // one of [-p, -k, -b]
            String keyHex = null
            String password = null
            config.keySource = null
            if (options.p) {
                config.keySource = Configuration.KeySource.PASSWORD
                password = options.getOptionValue("p")
            }
            if (options.k) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [-b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.KEY_HEX
                keyHex = options.getOptionValue("k")
            }

            if (config.keySource) {
                config.key = ToolUtilities.determineKey(keyHex, password, Configuration.KeySource.PASSWORD == config.keySource)
            }

            if (options.b) {
                if (config.keySource != null) {
                    throw new RuntimeException("Invalid options: Only one of [-b, -p, -k] is allowed for specifying the decryption password/key.")
                }
                config.keySource = Configuration.KeySource.BOOTSTRAP_FILE
                config.inputBootstrapPath = options.b

                logger.debug("Checking expected NiFi Registry bootstrap.conf format")
                config.key = BootstrapUtil.extractKeyFromBootstrapFile(config.inputBootstrapPath, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY)

                // check we have found the key
                if (config.key) {
                    logger.debug("Root key found in ${config.inputBootstrapPath}. This key will be used for decryption operations.")
                } else {
                    logger.warn("Bootstrap Conf flag present, but root key could not be found in ${config.inputBootstrapPath}.")
                }
            }

            config.decryptionProvider = StandardSensitivePropertyProviderFactory
                    .withKeyAndBootstrapSupplier(config.key, NiFiRegistryMode.getBootstrapSupplier(config.inputBootstrapPath))
                    .getProvider(config.protectionScheme)

            run(config)

        } catch (Exception e) {
            if (verboseEnabled) {
                logger.error("Encountered an error: ${e.getMessage()}", e)
            }
            EncryptConfigMain.printUsageAndExit(e.getMessage(), EncryptConfigMain.EXIT_STATUS_FAILURE)
        }
    }

}
