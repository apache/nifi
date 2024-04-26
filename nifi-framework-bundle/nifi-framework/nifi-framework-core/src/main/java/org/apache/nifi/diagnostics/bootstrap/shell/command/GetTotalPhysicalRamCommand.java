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
package org.apache.nifi.diagnostics.bootstrap.shell.command;

import org.apache.nifi.diagnostics.bootstrap.shell.result.SingleLineResult;

public class GetTotalPhysicalRamCommand extends AbstractShellCommand {
    private static final String COMMAND_NAME = "GetTotalPhysicalRam";
    private static final String RESULT_LABEL = "Total size of physical RAM";
    private static final String[] GET_TOTAL_PHYSICAL_RAM_FOR_MAC = new String[] {"/bin/sh", "-c", "sysctl -n hw.memsize"};
    private static final String[] GET_TOTAL_PHYSICAL_RAM_FOR_LINUX = new String[] {"/bin/sh", "-c", "free | grep Mem | awk '{print $2}'"};
    private static final String[] GET_TOTAL_PHYSICAL_RAM_FOR_WINDOWS = new String[] {"powershell.exe", "(Get-CimInstance Win32_OperatingSystem |" +
            " ft -hideTableHeaders TotalVisibleMemorySize | Out-String).trim()"};

    public GetTotalPhysicalRamCommand() {
        super(COMMAND_NAME,
                GET_TOTAL_PHYSICAL_RAM_FOR_WINDOWS,
                GET_TOTAL_PHYSICAL_RAM_FOR_LINUX,
                GET_TOTAL_PHYSICAL_RAM_FOR_MAC,
                new SingleLineResult(RESULT_LABEL, COMMAND_NAME)
        );
    }
}
