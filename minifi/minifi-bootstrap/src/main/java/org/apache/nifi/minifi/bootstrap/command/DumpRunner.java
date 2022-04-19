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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CMD_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.DEFAULT_LOGGER;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;

public class DumpRunner implements CommandRunner {
    private static final String DUMP_CMD = "DUMP";

    private final MiNiFiCommandSender miNiFiCommandSender;
    private final CurrentPortProvider currentPortProvider;

    public DumpRunner(MiNiFiCommandSender miNiFiCommandSender, CurrentPortProvider currentPortProvider) {
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.currentPortProvider = currentPortProvider;
    }

    /**
     * Writes a MiNiFi thread dump to the given file; if file is null, logs at
     * INFO level instead.
     *
     * @param args the second parameter is the file to write the dump content to
     */
    @Override
    public int runCommand(String[] args) {
        return dump(getArg(args, 1).map(File::new).orElse(null));
    }

    private int dump(File dumpFile) {
        Integer port = currentPortProvider.getCurrentPort();
        if (port == null) {
            CMD_LOGGER.error("Apache MiNiFi is not currently running");
            return MINIFI_NOT_RUNNING.getStatusCode();
        }

        Optional<String> dump;
        try {
            dump = miNiFiCommandSender.sendCommand(DUMP_CMD, port);
        } catch (IOException e) {
            CMD_LOGGER.error("Failed to get DUMP response from MiNiFi");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        }

        return Optional.ofNullable(dumpFile)
            .map(dmp -> writeDumpToFile(dmp, dump))
            .orElseGet(() -> {
                dump.ifPresent(CMD_LOGGER::info);
                return OK.getStatusCode();
            });
    }

    private Integer writeDumpToFile(File dumpFile, Optional<String> dump) {
        try (FileOutputStream fos = new FileOutputStream(dumpFile)) {
            fos.write(dump.orElse("Dump has empty response").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            CMD_LOGGER.error("Failed to write DUMP response to file");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        }
        // we want to log to the console (by default) that we wrote the thread dump to the specified file
        CMD_LOGGER.info("Successfully wrote thread dump to {}", dumpFile.getAbsolutePath());
        return OK.getStatusCode();
    }

    private Optional<String> getArg(String[] args, int index) {
        return Optional.ofNullable(args.length > index ? args[index] : null);
    }
}
