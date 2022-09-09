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

import org.apache.nifi.PropertyEncryptorCommand;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.util.console.utils.BaseCommandParameters;
import org.apache.nifi.util.console.utils.SchemeCandidates;
import org.apache.nifi.util.console.utils.SchemeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(name = "config",
        description = "Operate on application configs (nifi.properties etc)",
        usageHelpWidth=140
)
class ConfigSubcommand extends BaseCommandParameters implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConfigSubcommand.class);

    @CommandLine.ParentCommand
    private BaseCLICommand parent;

    @CommandLine.Parameters(
            completionCandidates = SchemeCandidates.class,
            converter = SchemeConverter.class,
            description="The encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
    ProtectionScheme scheme;

    @Override
    public void run() {
        final PropertyEncryptorCommand propertyEncryptorCommand = new PropertyEncryptorCommand(baseDirectory, rootPassphrase);
        if (parent instanceof PropertyEncryptorEncrypt) {
            encryptConfiguration(propertyEncryptorCommand);
        } else if (parent instanceof PropertyEncryptorDecrypt) {
            logger.info(RUN_LOG_MESSAGE, DECRYPT, baseDirectory);
        } else if (parent instanceof PropertyEncryptorMigrate) {
            logger.info(RUN_LOG_MESSAGE, MIGRATE, baseDirectory);
        }
    }

    private void encryptConfiguration(final PropertyEncryptorCommand propertyEncryptorCommand) {
        logger.info(RUN_LOG_MESSAGE, ENCRYPT, baseDirectory);
        propertyEncryptorCommand.encryptXmlConfigurationFiles(baseDirectory, scheme);
        try {
            propertyEncryptorCommand.encryptPropertiesFile(scheme);
            propertyEncryptorCommand.outputKeyToBootstrap();
        } catch (IOException e) {
            logger.error("Encrypting configuration files has failed due to: ", e);
        }
    }

    private void migrateConfg
}
