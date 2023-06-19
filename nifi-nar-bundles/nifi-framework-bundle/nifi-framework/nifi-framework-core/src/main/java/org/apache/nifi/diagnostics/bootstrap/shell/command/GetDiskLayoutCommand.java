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

import org.apache.nifi.diagnostics.bootstrap.shell.result.LineSplittingResult;

import java.util.Arrays;
import java.util.List;

public class GetDiskLayoutCommand extends AbstractShellCommand {
    private static final String COMMAND_NAME = "GetDiskLayout";
    private static final List<String> RESULT_LABELS = Arrays.asList("FileSystem/DeviceId", "Total size", "Used", "Free");
    private static final String REGEX_FOR_SPLITTING = "\\s+";
    private static final String[] GET_DISK_LAYOUT_FOR_MAC = new String[] {"/bin/sh", "-c", "df -Ph | sed 1d"};
    private static final String[] GET_DISK_LAYOUT_FOR_LINUX = new String[] {"/bin/sh", "-c", "df -h --output=source,size,used,avail,target -x tmpfs -x devtmpfs | sed 1d"};
    private static final String[] GET_DISK_LAYOUT_FOR_WINDOWS = new String[] {"powershell.exe", "Get-CIMInstance Win32_LogicalDisk " +
            "| ft -hideTableHeaders DeviceId, @{n='Size/GB'; e={[math]::truncate($_.Size/1GB)}}," +
            " @{n='Used/GB'; e={[math]::truncate($_.Size/1GB - $_.FreeSpace/1GB)}}, @{n='FreeSpace/GB'; e={[math]::truncate($_.freespace/1GB)}}, VolumeName"};

    public GetDiskLayoutCommand() {
        super(COMMAND_NAME,
                GET_DISK_LAYOUT_FOR_WINDOWS,
                GET_DISK_LAYOUT_FOR_LINUX,
                GET_DISK_LAYOUT_FOR_MAC,
                new LineSplittingResult(REGEX_FOR_SPLITTING, RESULT_LABELS, COMMAND_NAME)
        );
    }
}
