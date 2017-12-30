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

    static final int HELP_FORMAT_WIDTH = 160

    private Map<String, ToolMode> toolModesByName
    private LegacyMode legacyMode
    private CliBuilder cli

    EncryptConfig() {
        toolModesByName = new HashMap<>()
        addMode(new NiFiRegistryMode())
        addMode(new DecryptMode())
        legacyMode = new LegacyMode()
        addMode(legacyMode)
    }

    private void addMode(ToolMode mode) {
        this.toolModesByName.put(mode.getModeName(), mode)
    }

    private printUsageAndExit(int exitCode) {
        List<String> modes = this.toolModesByName.keySet().sort()
        String modesString = modes.join(", ")

        String header = "\nThis tool enables easy encryption and decryption of configuration files for NiFi and its sub-projects. " +
                "Unprotected files can be input to this tool to be protected by a key in a manner that is understood by NiFi. " +
                "Protected files, along with a key, can be input to this tool to be unprotected, for troubleshooting purposes.\n\n"

        def options = new Options()
        options.addOption("h", "help", false, "Show usage information (this message)")

        List<String> footerLines = [ "", "Available modes are: [${modesString}]", "" ]
        modes.forEach({
            footerLines.add("${it}:")
            footerLines.add("    ${toolModesByName.get(it).getModeDescription()}")
            footerLines.add("")
        })
        footerLines.add("For mode usage information, run:")
        footerLines.add("    encrypt-config <mode> -h")
        String footer = footerLines.join("\n")

        HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.printHelp("${EncryptConfig.class.getCanonicalName()} [-h] <mode> <mode_args>...", header, options, footer, false)

        System.exit(exitCode);
    }

    public static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider())

        def tool = new EncryptConfig()

        if (args.length < 1) {
            tool.printUsageAndExit(EXIT_STATUS_FAILURE)
        }

        String toolModeName = args[0]

        if (toolModeName.equals("-h") || toolModeName.equals("--help")) {
            tool.printUsageAndExit(EXIT_STATUS_OTHER)
        }

        if (!tool.toolModesByName.keySet().contains(toolModeName)) {
            // for backwards compatibility, if the first arg, indicating the mode or command, is not recognized as an explicit mode,
            // check if the arguments are understood by the old tool.
            // If so, add an implicit "legacy" mode to the args
            if (tool.legacyMode.matchesArgs(args)) {
                List<String> argsList = args.toList()
                argsList.add(0, tool.legacyMode.getModeName())
                args = (String[]) argsList.toArray()
            } else {
                tool.printUsageAndExit(EXIT_STATUS_FAILURE)
            }
        }

        String[] passThroughArgs = args.length >=2 ? args[(1..-1)] : []
        ToolMode toolMode = tool.toolModesByName.get(toolModeName)
        toolMode.run(passThroughArgs)

        System.exit(EXIT_STATUS_SUCCESS)
    }

}
