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
package org.apache.nifi.runtime.command;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.runtime.ManagementServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Shutdown Command registered as a Shutdown Hook
 */
public class ShutdownCommand implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ShutdownCommand.class);

    private final NiFiServer applicationServer;

    private final ManagementServer managementServer;

    private final Runnable diagnosticsCommand;

    public ShutdownCommand(final NiFiServer applicationServer, final ManagementServer managementServer, final Runnable diagnosticsCommand) {
        this.applicationServer = Objects.requireNonNull(applicationServer, "Application Server required");
        this.managementServer = Objects.requireNonNull(managementServer, "Management Server required");
        this.diagnosticsCommand = Objects.requireNonNull(diagnosticsCommand, "Diagnostics Command required");
    }

    @Override
    public void run() {
        logger.info("Application shutdown started");

        try {
            diagnosticsCommand.run();
        } catch (final Throwable e) {
            logger.warn("Diagnostics Command failed", e);
        }

        try {
            managementServer.stop();
        } catch (final Throwable e) {
            logger.warn("Management Server shutdown failed", e);
        }

        try {
            applicationServer.stop();
        } catch (final Throwable e) {
            logger.warn("Application Server shutdown failed", e);
        }

        logger.info("Application shutdown completed");
    }
}
