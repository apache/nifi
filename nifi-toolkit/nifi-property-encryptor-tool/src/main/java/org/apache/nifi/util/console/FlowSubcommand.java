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

import java.io.File;
import org.apache.nifi.properties.scheme.PropertyProtectionScheme;

@CommandLine.Command(name = "flow",
        description = "Operate on flow definition",
        usageHelpWidth=180
)
class FlowSubcommand {

//    @CommandLine.Parameters(description="Choose one of [@|bold ${COMPLETION-CANDIDATES}|@] to encrypt either application configuration files (nifi.properties etc) or flow.xml/flow.json file")
//    FileSelectionMode fileSelectionMode;
//
//    @CommandLine.Parameters(description="The base directory of NiFi/NiFi Registry/MiNiFi")
//    File baseDirectory;
//
//    @CommandLine.Parameters(description="The passphrase used to derive a key and encrypt files")
//    String passphrase;
//
//    @CommandLine.Parameters(description="The encryption scheme to use from")
//    PropertyProtectionScheme scheme;



}
