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

@CommandLine.Command(name = "encrypt",
        subcommands = {ConfigSubcommand.class, FlowSubcommand.class},
        description = "@|bold,fg(blue) Encrypt|@ the sensitive properties in either the application configuration files or the flow definition file.",
        usageHelpWidth=180
)
class PropertyEncryptorEncrypt extends DefaultCLIOptions implements Runnable {


    // ./property-encryptor.sh encrypt [config | flow] [root-nifi-dir | root-nifi-registry-dir | root-minifi-dir] [password] --scheme

//    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = {
//            "Print usage guide (this message)"})
//    private boolean helpRequested;
//
//    class PositionalParameters {
//        @CommandLine.Parameters(hidden = true)  // "hidden": don't show this parameter in usage help message
//        List<String> allParameters; // no "index" attribute: captures _all_ arguments (as Strings)
//
////        @CommandLine.Parameters(description="The encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
////        PropertyProtectionScheme scheme;
//
////        @Parameters(index = "1")    int port;
////        @Parameters(index = "2..*") File[] files;
//    }


//    @CommandLine.Parameters(index="1", description="The base directory of NiFi/NiFi Registry/MiNiFi")
//    File baseDirectory;
//
//    @CommandLine.Parameters(index="2", description="The passphrase used to derive a key and encrypt files")
//    String passphrase;
//
//    @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
//    FlowEncryptionScheme group;
//
//    @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
//    ConfigEncryptionScheme secondGroup;
//
//    static class ConfigEncryptionScheme {
//        @CommandLine.Parameters(index="0", description="Choose either [@|bold ${COMPLETION-CANDIDATES}|@]")
//        Operation fileType;
//
//        @CommandLine.Parameters(index="3", description="The config encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
//        PropertyProtectionScheme scheme;
//    }
//
//    static class FlowEncryptionScheme {
//        @CommandLine.Parameters(index="0", description="Choose either [@|bold ${COMPLETION-CANDIDATES}|@]")
//        Operation fileType;
//
//        @CommandLine.Parameters(index="3", description="The flow encryption scheme to use, from one of the following schemes: [@|bold ${COMPLETION-CANDIDATES}|@]")
//        FlowEncryptionScheme scheme;
//    }
//
//    static class Scheme {
//        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "Config encryption mode args%n")
//        ConfigEncryptionScheme configEncryptionScheme;
//
//        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "Flow encryption mode args%n")
//        FlowEncryptionScheme flowEncryptionScheme;
//    }
//
//    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
//    Scheme scheme;

    @Override
    public void run() {
        System.out.println("Encrypt command running..");
    }

}
