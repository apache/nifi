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
import org.apache.commons.cli.Options
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

class EncryptConfig {

    private static final Logger logger = LoggerFactory.getLogger(EncryptConfig.class)

    static final int EXIT_STATUS_SUCCESS = 0
    static final int EXIT_STATUS_FAILURE = -1
    static final int EXIT_STATUS_OTHER = 1

    static final String NIFI_REGISTRY_FLAG = "--nifi-registry"
    static final String DECRYPT_FLAG = "--decrypt"

    static final int HELP_FORMAT_WIDTH = 160

//    private Map<String, ToolMode> toolModesByName
//    private LegacyMode legacyMode
//    private CliBuilder cli

    EncryptConfig() {
//        toolModesByName = new HashMap<>()
//        addMode(new NiFiRegistryMode())
//        addMode(new DecryptMode())
//        legacyMode = new LegacyMode()
//        addMode(legacyMode)
    }

//    private void addMode(ToolMode mode) {
//        this.toolModesByName.put(mode.getModeName(), mode)
//    }

    private static printUsageAndExit(int exitCode) {
        String header = "\nThis tool enables easy encryption and decryption of configuration files for NiFi and its sub-projects. " +
                "Unprotected files can be input to this tool to be protected by a key in a manner that is understood by NiFi. " +
                "Protected files, along with a key, can be input to this tool to be unprotected, for troubleshooting purposes.\n\n"

        def options = new Options()
        options.addOption("h", "help", false, "Show usage information (this message)")

        // TODO: Print nifi mode usage

        // TODO: Print nifi-registry mode usage

        HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.printHelp("${EncryptConfig.class.getCanonicalName()} [-h] <args>...", header, options, "")

        System.exit(exitCode)
    }

    static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider())

        def tool = new EncryptConfig()

        if (args.length < 1) {
            tool.printUsageAndExit(EXIT_STATUS_FAILURE)
        }

        String firstArg = args[0]

        if (["-h", "--help"].contains(firstArg)) {
            tool.printUsageAndExit(EXIT_STATUS_OTHER)
        }

        ToolMode toolMode = determineModeFromArgs(args)
        if (toolMode) {
            toolMode.run(args)
            System.exit(EXIT_STATUS_SUCCESS)
        } else {
            printUsageAndExit(EXIT_STATUS_FAILURE)
        }
    }

    static ToolMode determineModeFromArgs(String[] args) {
        if (args.contains(NIFI_REGISTRY_FLAG)) {
            if (args.contains(DECRYPT_FLAG)) {
                return new DecryptMode()
            } else {
                return new NiFiRegistryMode()
            }
        } else {
            if (args.contains(DECRYPT_FLAG)) {
                logger.error("The ${DECRYPT_FLAG} flag is only available when running in ${NIFI_REGISTRY_FLAG} mode and targeting nifi-registry.properties to allow for the inline TLS status check. ")
                return null
            } else {
                return new LegacyMode()
            }
        }
    }
}
