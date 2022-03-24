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
import org.apache.nifi.properties.ConfigEncryptionTool
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

class EncryptConfigMain {

    private static final Logger logger = LoggerFactory.getLogger(EncryptConfigMain.class)

    static final int EXIT_STATUS_SUCCESS = 0
    static final int EXIT_STATUS_FAILURE = -1
    static final int EXIT_STATUS_OTHER = 1

    static final String NIFI_REGISTRY_OPT = "nifiRegistry"
    static final String NIFI_REGISTRY_FLAG = "--${NIFI_REGISTRY_OPT}".toString()
    static final String DECRYPT_OPT = "decrypt"
    static final String DECRYPT_FLAG = "--${DECRYPT_OPT}".toString()

    static final int HELP_FORMAT_WIDTH = 160

    // Access should only be through static methods
    private EncryptConfigMain() {
    }

    static printUsage(String message = "") {

        if (message) {
            System.out.println(message)
            System.out.println()
        }

        String header = "\nThis tool enables easy encryption and decryption of configuration files for NiFi and its sub-projects. " +
                "Unprotected files can be input to this tool to be protected by a key in a manner that is understood by NiFi. " +
                "Protected files, along with a key, can be input to this tool to be unprotected, for troubleshooting or automation purposes.\n\n"

        def options = new Options()
        options.addOption("h", "help", false, "Show usage information (this message)")
        options.addOption(null, NIFI_REGISTRY_OPT, false, "Specifies to target NiFi Registry. When this flag is not included, NiFi is the target.")

        HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.setOptionComparator(null)
        helpFormatter.printHelp("${EncryptConfigMain.class.getCanonicalName()} [-h] [options]", header, options, "\n")
        System.out.println()

        helpFormatter.setSyntaxPrefix("") // disable "usage: " prefix for the following outputs

        Options nifiModeOptions = ConfigEncryptionTool.getCliOptions()
        helpFormatter.printHelp(
                "When targeting NiFi:",
                nifiModeOptions,
                false)
        System.out.println()

        Options nifiRegistryModeOptions = NiFiRegistryMode.getCliOptions()
        nifiRegistryModeOptions.addOption(null, DECRYPT_OPT, false, "Can be used with -r to decrypt a previously encrypted NiFi Registry Properties file. Decrypted content is printed to STDOUT.")
        helpFormatter.printHelp(
                "When targeting NiFi Registry using the ${NIFI_REGISTRY_FLAG} flag:",
                nifiRegistryModeOptions,
                false)
        System.out.println()

    }

    static void printUsageAndExit(String message = "", int exitStatusCode) {
        printUsage(message)
        System.exit(exitStatusCode)
    }

    static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider())

        if (args.length < 1) {
            printUsageAndExit(EXIT_STATUS_FAILURE)
        }

        String firstArg = args[0]

        if (["-h", "--help"].contains(firstArg)) {
            printUsageAndExit(EXIT_STATUS_OTHER)
        }

        try {
            List<String> argsList = args
            ToolMode toolMode = determineModeFromArgs(argsList)
            if (toolMode) {
                toolMode.run((String[])argsList.toArray())
                System.exit(EXIT_STATUS_SUCCESS)
            } else {
                printUsageAndExit(EXIT_STATUS_FAILURE)
            }
        } catch (Throwable t) {
            logger.error("", t)
            printUsageAndExit(t.getMessage(), EXIT_STATUS_FAILURE)
        }
    }

    static ToolMode determineModeFromArgs(List<String> args) {
        if (args.contains(NIFI_REGISTRY_FLAG)) {
            args.remove(NIFI_REGISTRY_FLAG)
            if (args.contains(DECRYPT_FLAG)) {
                args.remove(DECRYPT_FLAG)
                return new NiFiRegistryDecryptMode()
            } else {
                return new NiFiRegistryMode()
            }
        } else {
            if (args.contains(DECRYPT_FLAG)) {
                logger.error("The ${DECRYPT_FLAG} flag is only available when running in ${NIFI_REGISTRY_FLAG} mode and targeting nifi-registry.properties to allow for the inline TLS status check.")
                return null
            } else {
                return new LegacyMode()
            }
        }
    }
}
