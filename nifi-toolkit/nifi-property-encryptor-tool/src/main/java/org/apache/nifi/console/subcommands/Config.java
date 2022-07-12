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
package org.apache.nifi.console.subcommands;

import org.apache.nifi.PropertyEncryptorMain;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Path;

/**
 * The 'config' subcommand defines the operations that can be performed on the application configuration (eg. nifi.properties, login-identity-providers.xml etc)
 */
@CommandLine.Command(name="config",
        description="Subcommand to operate on the application configuration files (eg. nifi.properties, login-identity-providers.xml etc)",
        usageHelpWidth = 140,
        subcommands = { Config.Encrypt.class, Config.Decrypt.class, Config.Migrate.class }
)
public class Config implements Runnable {

//          ./property-encryptor.sh config <encrypt | decrypt | migrate> base-nifi-dir password encryption-scheme

//    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
//    ConfigCommonParameters encrypt;

    static class BaseDirectory {
        @CommandLine.Parameters(index="0", paramLabel="baseDirectory", description="The base directory of NiFi/NiFi Registry/MiNiFi")
        Path baseDirectory;
    }

    static class Password {
        @CommandLine.Parameters(index="1", paramLabel="password", description="The password to use")
        String password;
    }

    static class Scheme {
        @CommandLine.Parameters(index="2", paramLabel="scheme", description = "The config encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
        PropertyProtectionScheme scheme;
    }

    static class Common {
        @CommandLine.ArgGroup(multiplicity = "1")
        BaseDirectory baseDirectory;

        @CommandLine.ArgGroup(multiplicity = "1")
        Password password;
    }

//    static class ConfigCommonParameters {
//        @CommandLine.Parameters(index="0", description="The base directory of NiFi/NiFi Registry/MiNiFi")
//        File baseDirectory;
//
//        // TODO: Should this be interactive to keep secret? https://picocli.info/man/3.x/#_interactive_password_options
//        @CommandLine.Parameters(index="1", description="The passphrase to use")
//        String passphrase;
//    }

    @CommandLine.Command(name = "encrypt", description = "Encrypt config files")
    static class Encrypt implements Runnable {

        // TO ENCRYPT: ./property-encryptor.sh config encrypt /nifi secretpass AES_GCM

        @CommandLine.Mixin()
        Common configParameters;

        @CommandLine.Mixin()
        Scheme scheme;


//
//        @CommandLine.Mixin()
//        Scheme scheme;

//        @CommandLine.Parameters(index="2", description="The config encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
//        PropertyProtectionScheme scheme;

        // TODO: Do we need more parameters for scheme details or do we provide a file which contains the relevant config for the chosen scheme provider?

        @Override
        public void run() {
            System.out.println(String.format("The property encryptor is running the encrypt command on the given base directory [%s]", configParameters.baseDirectory.baseDirectory));
            PropertyEncryptorMain main = new PropertyEncryptorMain(configParameters.baseDirectory.baseDirectory, configParameters.password.password);
            //main.encryptConfigurationFiles(configParameters.baseDirectory.baseDirectory, scheme.scheme);
        }
    }

    @CommandLine.Command(name = "decrypt", description = "Decrypt config files")
    static class Decrypt implements Runnable {

        @CommandLine.Mixin
        Common configParameters;

        @Override
        public void run() {
            System.out.println("Decrypt");
        }
    }

    @CommandLine.Command(name = "migrate", description = "Migrate config files")
    static class Migrate implements Runnable {

        // [root-nifi-dir | root-nifi-registry-dir | root-minifi-dir] [new-password] [new-scheme]

//        @CommandLine.ArgGroup() {
//
//        }

        @Override
        public void run() {
            System.out.println("Migrate");
        }
    }

    @Override
    public void run() {
        System.out.println("Incomplete usage of 'config' command, please supply ENCRYPT|DECRYPT|MIGRATE");
    }
}