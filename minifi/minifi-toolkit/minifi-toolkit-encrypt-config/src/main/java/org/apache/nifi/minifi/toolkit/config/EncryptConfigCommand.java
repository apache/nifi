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
package org.apache.nifi.minifi.toolkit.config;

import org.apache.nifi.minifi.toolkit.config.command.MiNiFiEncryptConfig;
import picocli.CommandLine;

/**
 * Encrypt Config Command launcher for Command Line implementation
 */
public class EncryptConfigCommand {

    /**
     * Main command method launches Picocli Command Line implementation of Encrypt Config
     *
     * @param arguments Command line arguments
     */
    public static void main(String[] arguments) {
        CommandLine commandLine = new CommandLine(new MiNiFiEncryptConfig());
        if (arguments.length == 0) {
            commandLine.usage(System.out);
        } else {
            int status = commandLine.execute(arguments);
            System.exit(status);
        }
    }

}
