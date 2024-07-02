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
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.OK;

import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;

public class FlowStatusRunner implements CommandRunner {
    private final PeriodicStatusReporterManager periodicStatusReporterManager;

    public FlowStatusRunner(PeriodicStatusReporterManager periodicStatusReporterManager) {
        this.periodicStatusReporterManager = periodicStatusReporterManager;
    }

    /**
     * Receive and print detailed flow information from MiNiFi.
     * Example query: processor:TailFile:health,stats,bulletins
     * @param args the input arguments
     * @return status code
     */
    @Override
    public int runCommand(String[] args) {
        if (args.length == 2) {
            CMD_LOGGER.info("{}", periodicStatusReporterManager.statusReport(args[1]));
            return OK.getStatusCode();
        } else {
            CMD_LOGGER.error("The 'flowStatus' command requires an input query. See the System Admin Guide 'FlowStatus Script Query' section for complete details.");
            return ERROR.getStatusCode();
        }
    }
}
