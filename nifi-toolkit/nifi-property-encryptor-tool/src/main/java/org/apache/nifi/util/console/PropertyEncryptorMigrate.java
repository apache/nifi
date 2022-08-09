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

@CommandLine.Command(name = "migrate",
        description = "@|bold,fg(blue) Migrate|@ already encrypted application configuration or flow.xml.gz files to a new password. Can also apply a new scheme if provided.",
        usageHelpWidth=140)
class PropertyEncryptorMigrate extends BaseCLICommand implements Runnable  {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        // Show usage information if a subcommand was not chosen
        spec.commandLine().usage(System.err);
    }
}
