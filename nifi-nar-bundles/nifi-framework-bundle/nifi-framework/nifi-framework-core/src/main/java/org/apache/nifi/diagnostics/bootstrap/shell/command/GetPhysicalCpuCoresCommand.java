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

public class GetPhysicalCpuCoresCommand extends AbstractShellCommand {
    private static final String COMMAND_NAME = "GetPhysicalCpuCores";
    private static final String RESULT_LABEL = "Number of physical CPU core(s)";
    private static final String[] GET_CPU_CORE_FOR_MAC = new String[] {"/bin/sh", "-c", "sysctl -n hw.physicalcpu"};
    private static final String[] GET_CPU_CORE_FOR_LINUX = new String[] {"/bin/sh", "-c", "lscpu -b -p=Core,Socket | grep -v '^#' | sort -u | wc -l"};
    private static final String[] GET_CPU_CORE_FOR_WINDOWS = new String[] {"powershell.exe", "(Get-CIMInstance Win32_processor | ft NumberOfCores -hideTableHeaders | Out-String).trim()"};

    public GetPhysicalCpuCoresCommand() {
        super(COMMAND_NAME,
                GET_CPU_CORE_FOR_WINDOWS,
                GET_CPU_CORE_FOR_LINUX,
                GET_CPU_CORE_FOR_MAC,
                new SingleLineResult(RESULT_LABEL, COMMAND_NAME)
        );
    }
}
