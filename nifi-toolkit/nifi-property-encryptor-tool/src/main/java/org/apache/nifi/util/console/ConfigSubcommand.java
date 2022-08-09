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
package org.apache.nifi.util.console;

import org.apache.nifi.PropertyEncryptorMain;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;
import org.apache.nifi.util.console.utils.BaseCommandParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "config",
        description = "Operate on application configs",
        usageHelpWidth=140
)
class ConfigSubcommand extends BaseCommandParameters implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConfigSubcommand.class);
    private static final String runMessage = "The property encryptor is running to [{}] configuration files in [{}]";

    @CommandLine.ParentCommand
    private BaseCLICommand parent;

    @CommandLine.Parameters(description="The encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
    PropertyProtectionScheme scheme;

    @Override
    public void run() {
        final PropertyEncryptorMain propertyEncryptorMain = new PropertyEncryptorMain(baseDirectory, passphrase);
        if (parent instanceof PropertyEncryptorEncrypt) {
            logger.info(runMessage, "encrypt", baseDirectory);
            propertyEncryptorMain.encryptConfigurationFiles(baseDirectory, scheme);
        } else if (parent instanceof PropertyEncryptorDecrypt) {
            logger.info(runMessage, "decrypt", baseDirectory);
        } else if (parent instanceof PropertyEncryptorMigrate) {
            logger.info(runMessage, "migrate", baseDirectory);
        }
    }
}
