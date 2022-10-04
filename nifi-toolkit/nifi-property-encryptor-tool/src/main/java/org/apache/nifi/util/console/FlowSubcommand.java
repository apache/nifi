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
import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.util.console.utils.BaseCommandParameters;
import org.apache.nifi.util.console.utils.PropertyEncryptionMethodCandidates;
import org.apache.nifi.util.console.utils.PropertyEncryptionMethodConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "flow",
        description = "Operate on flow definition (flow.xml)",
        usageHelpWidth=140
)
class FlowSubcommand extends BaseCommandParameters implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PropertyEncryptorCLI.class);

    @CommandLine.ParentCommand
    private BaseCLICommand parent;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Parameters(
            completionCandidates = PropertyEncryptionMethodCandidates.class,
            converter = PropertyEncryptionMethodConverter.class,
            description="The encryption method to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
    PropertyEncryptionMethod encryptionMethod;

    @Override
    public void run() {
        final PropertyEncryptorCommand propertyEncryptorCommand = new PropertyEncryptorCommand(baseDirectory, rootPassphrase);
        if (parent instanceof PropertyEncryptorEncrypt) {
            encryptFlowDefinition(propertyEncryptorCommand);
        } else if (parent instanceof PropertyEncryptorDecrypt) {
            logger.info(RUN_LOG_MESSAGE, DECRYPT, baseDirectory);
        } else if (parent instanceof PropertyEncryptorMigrate) {
            logger.info(RUN_LOG_MESSAGE, MIGRATE, baseDirectory);
        }
    }

    private void encryptFlowDefinition(final PropertyEncryptorCommand propertyEncryptorCommand) {
        propertyEncryptorCommand.encryptFlowDefinition(encryptionMethod, rootPassphrase);
    }
}
