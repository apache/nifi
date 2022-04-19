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
package org.apache.nifi.minifi.bootstrap.service;

import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.STATUS_REPORTER_COMPONENTS_KEY;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.MiNiFiStatus;
import org.apache.nifi.minifi.bootstrap.QueryableStatusAggregator;
import org.apache.nifi.minifi.bootstrap.status.PeriodicStatusReporter;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicStatusReporterManager implements QueryableStatusAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicStatusReporterManager.class);
    private static final String FLOW_STATUS_REPORT_CMD = "FLOW_STATUS_REPORT";

    private final Properties bootstrapProperties;
    private final MiNiFiStatusProvider miNiFiStatusProvider;
    private final MiNiFiCommandSender miNiFiCommandSender;
    private final MiNiFiParameters miNiFiParameters;

    private Set<PeriodicStatusReporter> periodicStatusReporters = Collections.emptySet();

    public PeriodicStatusReporterManager(Properties bootstrapProperties, MiNiFiStatusProvider miNiFiStatusProvider, MiNiFiCommandSender miNiFiCommandSender,
        MiNiFiParameters miNiFiParameters) {
        this.bootstrapProperties = bootstrapProperties;
        this.miNiFiStatusProvider = miNiFiStatusProvider;
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.miNiFiParameters = miNiFiParameters;
    }

    public void startPeriodicNotifiers() {
        periodicStatusReporters = initializePeriodicNotifiers();

        for (PeriodicStatusReporter periodicStatusReporter: periodicStatusReporters) {
            periodicStatusReporter.start();
            LOGGER.debug("Started {} notifier", periodicStatusReporter.getClass().getCanonicalName());
        }
    }

    public void shutdownPeriodicStatusReporters() {
        LOGGER.debug("Initiating shutdown of bootstrap periodic status reporters...");
        for (PeriodicStatusReporter periodicStatusReporter : periodicStatusReporters) {
            try {
                periodicStatusReporter.stop();
            } catch (Exception exception) {
                LOGGER.error("Could not successfully stop periodic status reporter " + periodicStatusReporter.getClass() + " due to ", exception);
            }
        }
    }

    public FlowStatusReport statusReport(String statusRequest) {
        MiNiFiStatus status = miNiFiStatusProvider.getStatus(miNiFiParameters.getMiNiFiPort(), miNiFiParameters.getMinifiPid());

        List<String> problemsGeneratingReport = new LinkedList<>();
        if (!status.isProcessRunning()) {
            problemsGeneratingReport.add("MiNiFi process is not running");
        }

        if (!status.isRespondingToPing()) {
            problemsGeneratingReport.add("MiNiFi process is not responding to pings");
        }

        if (!problemsGeneratingReport.isEmpty()) {
            FlowStatusReport flowStatusReport = new FlowStatusReport();
            flowStatusReport.setErrorsGeneratingReport(problemsGeneratingReport);
            return flowStatusReport;
        }

        return getFlowStatusReport(statusRequest, status.getPort());
    }

    private Set<PeriodicStatusReporter> initializePeriodicNotifiers() {
        LOGGER.debug("Initiating bootstrap periodic status reporters...");
        Set<PeriodicStatusReporter> statusReporters = new HashSet<>();

        String reportersCsv = bootstrapProperties.getProperty(STATUS_REPORTER_COMPONENTS_KEY);

        if (reportersCsv != null && !reportersCsv.isEmpty()) {
            for (String reporterClassname : reportersCsv.split(",")) {
                try {
                    Class<?> reporterClass = Class.forName(reporterClassname);
                    PeriodicStatusReporter reporter = (PeriodicStatusReporter) reporterClass.newInstance();
                    reporter.initialize(bootstrapProperties, this);
                    statusReporters.add(reporter);
                    LOGGER.debug("Initialized {} notifier", reporterClass.getCanonicalName());
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new RuntimeException("Issue instantiating notifier " + reporterClassname, e);
                }
            }
        }
        return statusReporters;
    }

    private FlowStatusReport getFlowStatusReport(String statusRequest, int port) {
        FlowStatusReport flowStatusReport;
        try {
            flowStatusReport = miNiFiCommandSender.sendCommandForObject(FLOW_STATUS_REPORT_CMD, port, FlowStatusReport.class, statusRequest);
        } catch (Exception e) {
            flowStatusReport = new FlowStatusReport();
            String message = "Failed to get status report from MiNiFi due to:" + e.getMessage();
            flowStatusReport.setErrorsGeneratingReport(Collections.singletonList(message));
            LOGGER.error(message, e);
        }
        return flowStatusReport;
    }

}
