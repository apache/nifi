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
package org.apache.nifi.diagnostics.bootstrap.shell.commands;

import org.apache.nifi.diagnostics.DiagnosticUtils;
import org.apache.nifi.diagnostics.bootstrap.shell.results.ShellCommandResult;

public abstract class AbstractShellCommand {
    private final String name;
    private final String[] windowsCommand;
    private final String[] linuxCommand;
    private final String[] macCommand;
    private final ShellCommandResult parser;

    public AbstractShellCommand(final String name, final String[] windowsCommand, final String[] linuxCommand, final String[] macCommand, final ShellCommandResult parser) {
        this.name = name;
        this.windowsCommand = windowsCommand;
        this.linuxCommand = linuxCommand;
        this.macCommand = macCommand;
        this.parser = parser;
    }

    public String getName() {
        return name;
    }

    public String[] getCommand() {
        if (DiagnosticUtils.isMac()){
            return macCommand;
        } else if (DiagnosticUtils.isLinuxUnix()){
            return linuxCommand;
        } else if (DiagnosticUtils.isWindows()){
            return windowsCommand;
        } else {
            return new String[] {};
        }
    }

    public ShellCommandResult getParser() {
        return parser;
    }
}
