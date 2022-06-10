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
package org.apache.nifi.minifi.bootstrap;


import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownHook extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger("org.apache.nifi.minifi.bootstrap.Command");

    private final RunMiNiFi runner;
    private final MiNiFiStdLogHandler miNiFiStdLogHandler;

    public ShutdownHook(RunMiNiFi runner, MiNiFiStdLogHandler miNiFiStdLogHandler) {
        this.runner = runner;
        this.miNiFiStdLogHandler = miNiFiStdLogHandler;
    }

    @Override
    public void run() {
        LOGGER.info("Initiating Shutdown of MiNiFi...");

        miNiFiStdLogHandler.shutdown();
        runner.shutdownChangeNotifier();
        runner.getPeriodicStatusReporterManager().shutdownPeriodicStatusReporters();
        runner.setAutoRestartNiFi(false);
        runner.run(BootstrapCommand.STOP);
    }
}
