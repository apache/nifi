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
package org.apache.nifi.toolkit.encryptconfig.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BootstrapUtil {

    static final String NIFI_BOOTSTRAP_KEY_PROPERTY = "nifi.bootstrap.sensitive.key";
    static final String REGISTRY_BOOTSTRAP_KEY_PROPERTY = "nifi.registry.bootstrap.sensitive.key";

    private static final Logger logger = LoggerFactory.getLogger(BootstrapUtil.class)

    private static final String BOOTSTRAP_KEY_COMMENT = "# Master key in hexadecimal format for encrypted sensitive configuration values"

    /**
     * Tries to load keyHex from input bootstrap.conf
     *
     * @return keyHex, if present in input bootstrap file; otherwise, null
     */
    static String extractKeyFromBootstrapFile(String inputBootstrapPath, String bootstrapKeyPropertyName) throws IOException {

        File inputBootstrapConfFile = new File(inputBootstrapPath)
        if (!(inputBootstrapPath && ToolUtilities.canRead(inputBootstrapConfFile))) {
            throw new IOException("The bootstrap.conf file at ${inputBootstrapPath} must exist and be readable by the user running this tool")
        }

        String keyValue = null
        try {
            List<String> lines = inputBootstrapConfFile.readLines()
            int keyLineIndex = lines.findIndexOf { it.startsWith("${bootstrapKeyPropertyName}=") }

            if (keyLineIndex != -1) {
                logger.debug("The key property was detected in bootstrap.conf")
                String keyLine = lines[keyLineIndex]
                keyValue = keyLine.split("=", 2)[1]
                if (keyValue.trim().isEmpty()) {
                    keyValue = null
                }
            } else {
                logger.debug("The key property was not detected in input bootstrap.conf.")
            }


        } catch (IOException e) {
            logger.error("Encountered an exception reading the master key from the input bootstrap.conf file: ${e.getMessage()}")
            throw e
        }

        return keyValue;

    }

    /**
     * Writes key to output bootstrap.conf
     *
     * @param keyHex
     */
    static void writeKeyToBootstrapFile(String keyHex, String bootstrapKeyPropertyName, String outputBootstrapPath, String inputBootstrapPath) throws IOException {
        File inputBootstrapConfFile = new File(inputBootstrapPath)
        File outputBootstrapConfFile = new File(outputBootstrapPath)

        if (!ToolUtilities.canRead(inputBootstrapConfFile)) {
            throw new IOException("The bootstrap.conf file at ${inputBootstrapPath} must exist and be readable by the user running this tool")
        }

        if (!ToolUtilities.isSafeToWrite(outputBootstrapConfFile)) {
            throw new IOException("The bootstrap.conf file at ${outputBootstrapPath} must exist and be readable and writable by the user running this tool")
        }

        try {
            List<String> lines = inputBootstrapConfFile.readLines()

            updateBootstrapContentsWithKey(lines, keyHex, bootstrapKeyPropertyName)

            // Write the updated values to the output file
            outputBootstrapConfFile.text = lines.join("\n")
        } catch (IOException e) {
            logger.error("Encountered an exception reading the master key from the input bootstrap.conf file: ${e.getMessage()}")
            throw e
        }
    }


    /**
     * Accepts the lines of the {@code bootstrap.conf} file as a {@code List <String>} and updates or adds the key property (and associated comment).
     *
     * @param lines the lines of the bootstrap file
     * @return the updated lines
     */
    private static List<String> updateBootstrapContentsWithKey(List<String> lines, String newKeyHex, String bootstrapKeyPropertyName) {
        String keyLine = "${bootstrapKeyPropertyName}=${newKeyHex}"
        // Try to locate the key property line
        int keyLineIndex = lines.findIndexOf { it.startsWith("${bootstrapKeyPropertyName}=") }

        // If it was found, update inline
        if (keyLineIndex != -1) {
            logger.debug("The key property was detected in bootstrap.conf")
            lines[keyLineIndex] = keyLine
            logger.debug("The bootstrap key value was updated")

            // Ensure the comment explaining the property immediately precedes it (check for edge case where key is first line)
            int keyCommentLineIndex = keyLineIndex > 0 ? keyLineIndex - 1 : 0
            if (lines[keyCommentLineIndex] != BOOTSTRAP_KEY_COMMENT) {
                lines.add(keyCommentLineIndex, BOOTSTRAP_KEY_COMMENT)
                logger.debug("A comment explaining the bootstrap key property was added")
            }
        } else {
            // If it wasn't present originally, add the comment and key property
            lines.addAll(["\n", BOOTSTRAP_KEY_COMMENT, keyLine])
            logger.debug("The key property was not detected in bootstrap.conf so it was added along with a comment explaining it")
        }

        return lines
    }

}
