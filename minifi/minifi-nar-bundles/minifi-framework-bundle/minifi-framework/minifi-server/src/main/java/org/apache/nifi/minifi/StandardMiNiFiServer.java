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

package org.apache.nifi.minifi;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.headless.HeadlessNiFiServer;
import org.apache.nifi.minifi.bootstrap.BootstrapListener;
import org.apache.nifi.minifi.c2.C2NifiClientService;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.minifi.status.StatusConfigReporter;
import org.apache.nifi.minifi.status.StatusRequestException;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardMiNiFiServer extends HeadlessNiFiServer implements MiNiFiServer {

    private static final Logger logger = LoggerFactory.getLogger(StandardMiNiFiServer.class);
    public static final String BOOTSTRAP_PORT_PROPERTY = "nifi.bootstrap.listen.port";

    private BootstrapListener bootstrapListener;

    /* A reference to the client service for handling*/
    private C2NifiClientService c2NifiClientService;


    public StandardMiNiFiServer() {
        super();
    }

    public FlowStatusReport getStatusReport(String requestString) throws StatusRequestException {
        return StatusConfigReporter.getStatus(getFlowController(), requestString, logger);
    }

    @Override
    public void start() {
        super.start();

        initBootstrapListener();
        initC2();
        sendStartedStatus();
        startHeartbeat();
    }

    @Override
    public void stop(boolean reload) {
        super.stop();
        if (bootstrapListener != null) {
            try {
                if (reload) {
                    bootstrapListener.reload();
                } else {
                    bootstrapListener.stop();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (c2NifiClientService != null) {
            c2NifiClientService.stop();
        }
    }

    private void initC2() {
        if (Boolean.parseBoolean(getNiFiProperties().getProperty(MiNiFiProperties.C2_ENABLE.getKey(), MiNiFiProperties.C2_ENABLE.getDefaultValue()))) {
            NiFiProperties niFiProperties = getNiFiProperties();
            enabledFlowIngestors(niFiProperties).ifPresentOrElse(
                flowIngestors -> {
                    logger.warn("Due to enabled flow ingestor(s) [{}] C2 client is not created. Please disable flow ingestors when using C2", flowIngestors);
                    c2NifiClientService = null;
                },
                () -> {
                    logger.info("C2 enabled, creating a C2 client instance");
                    c2NifiClientService = new C2NifiClientService(niFiProperties, getFlowController(), bootstrapListener, getFlowService());
                });
        } else {
            logger.debug("C2 Property [{}] missing or disabled: C2 client not created", MiNiFiProperties.C2_ENABLE.getKey());
            c2NifiClientService = null;
        }
    }

    private Optional<String> enabledFlowIngestors(NiFiProperties niFiProperties) {
        return ofNullable(niFiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_NOTIFIER_INGESTORS.getKey(), EMPTY))
            .map(String::trim)
            .filter(StringUtils::isNotBlank);
    }

    private void startHeartbeat() {
        if (c2NifiClientService != null) {
            c2NifiClientService.start();
        }
    }

    private void initBootstrapListener() {
        String bootstrapPort = System.getProperty(BOOTSTRAP_PORT_PROPERTY);
        if (bootstrapPort != null) {
            try {
                int port = Integer.parseInt(bootstrapPort);

                if (port < 1 || port > 65535) {
                    throw new RuntimeException("Failed to start MiNiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
                }

                bootstrapListener = new BootstrapListener(this, port);
                NiFiProperties niFiProperties = getNiFiProperties();
                bootstrapListener.start(niFiProperties.getDefaultListenerBootstrapPort());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to start MiNiFi because of Bootstrap listener initialization error", e);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Failed to start MiNiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
            }
        } else {
            logger.info("MiNiFi started without Bootstrap Port information provided; will not listen for requests from Bootstrap");
            bootstrapListener = null;
        }
    }

    private void sendStartedStatus() {
        if (bootstrapListener != null) {
            try {
                bootstrapListener.sendStartedStatus(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
