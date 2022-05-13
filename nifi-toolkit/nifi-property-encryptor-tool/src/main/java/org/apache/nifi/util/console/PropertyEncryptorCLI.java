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

import picocli.CommandLine;

@CommandLine.Command(name = "PropertyEncryptor",
        usageHelpWidth=140,
        subcommands = {
        PropertyEncryptorEncrypt.class,
        PropertyEncryptorDecrypt.class,
        PropertyEncryptorMigrate.class,
        PropertyEncryptorTranslateForCLI.class}
)
class PropertyEncryptorCLI extends DefaultCLIOptions implements Runnable {

    /**
     * ./property-encryptor.sh --help
     * usage: org.apache.nifi.toolkit.propertyencryptor.PropertyEncryptorMain [-h] [-v] [encrypt | decrypt | migrate | translate-cli]
     *
     * This tool can be used to easily encrypt configuration files for NiFi and its sub-projects (NiFi Registry, MiNiFi), as well as the flow.xml.gz or flow.json.gz files. Given a root directory, password and scheme it will protect all secret values within configuration files or within the flow.xml.gz/flow.json.gz with the key/password. The tool can also decrypt configuration files given the correct credentials. It also allows for migrating the password used from old to new, and changing the encryption scheme used.
     *
     * -h,--help           Show usage information (this message)
     * -v,--verbose        Sets verbose mode (default false)
     *
     * Command examples:
     * ./property-encryptor.sh encrypt [config | flow] [root-nifi-dir | root-nifi-registry-dir | root-minifi-dir] [password] [scheme]
     * ./property-encryptor.sh decrypt config [root-nifi-dir | root-nifi-registry-dir | root-minifi-dir] [password] [scheme]
     * ./property-encryptor.sh migrate [config | flow] [root-nifi-dir | root-nifi-registry-dir | root-minifi-dir] [new-password] [new-scheme]
     * ./property-encryptor.sh translate-for-nifi-cli nifi.properties
     *
     * Encrypt examples:
     * ./property-encryptor.sh encrypt config root-nifi-dir passphrase AES-GCM
     *
     * OR
     *
     * ./property-encryptor.sh config <encrypt | decrypt | migrate>
     * ./property-encryptor.sh flow <encrypt | migrate> base-nifi-dir password [PBECipherProvider] [EncryptionMethod]
     * ./property-encryptor.sh translate-for-nifi-cli nifi.properties
     *
     *                                  sub-subcommand
     * ./property-encryptor.sh encrypt [config | flow]
     *
     */

    @Override
    public void run() {
        if (verboseLogging) {
            System.out.println("Verbose logging enabled");
        }
        System.out.println("Running the Property Encryptor...");
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new PropertyEncryptorCLI()).setCaseInsensitiveEnumValuesAllowed(true).execute(args));
    }
}