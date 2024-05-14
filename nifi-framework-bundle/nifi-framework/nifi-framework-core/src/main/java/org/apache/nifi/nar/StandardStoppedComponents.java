/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holder for tracking components that are stopped/disabled and need to be restarted.
 */
class StandardStoppedComponents implements StoppedComponents {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardStoppedComponents.class);

    private final Collection<ProcessorNode> processors = new HashSet<>();
    private final Collection<ControllerServiceNode> controllerServices = new HashSet<>();
    private final Collection<ReportingTaskNode> reportingTasks = new HashSet<>();
    private final Collection<FlowAnalysisRuleNode> flowAnalysisRules = new HashSet<>();

    private final ControllerServiceProvider controllerServiceProvider;

    public StandardStoppedComponents(final ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    @Override
    public void addProcessor(final ProcessorNode processor) {
        processors.add(processor);
    }

    @Override
    public void addControllerService(final ControllerServiceNode controllerService) {
        controllerServices.add(controllerService);
    }

    @Override
    public void addAllControllerServices(final Collection<ControllerServiceNode> controllerServices) {
        this.controllerServices.addAll(controllerServices);
    }

    @Override
    public void addReportingTask(final ReportingTaskNode reportingTask) {
        reportingTasks.add(reportingTask);
    }

    @Override
    public void addFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRule) {
        flowAnalysisRules.add(flowAnalysisRule);
    }

    @Override
    public void startAll() {
        LOGGER.debug("Starting/enabling components that were stopped/disabled for reloading...");

        final Set<ControllerServiceNode> servicesToEnable = controllerServices.stream()
                .filter(controllerServiceNode -> controllerServiceNode.getState() == ControllerServiceState.DISABLED)
                .collect(Collectors.toSet());
        servicesToEnable.forEach(controllerService -> LOGGER.debug("Enabling ControllerService with ID [{}]", controllerService.getIdentifier()));
        controllerServiceProvider.enableControllerServicesAsync(servicesToEnable);

        for (final ReportingTaskNode reportingTask : reportingTasks) {
            if (reportingTask.getScheduledState() == ScheduledState.STOPPED) {
                LOGGER.debug("Starting ReportingTask with ID {}", reportingTask.getIdentifier());
                reportingTask.start();
            }
        }

        for (final FlowAnalysisRuleNode flowAnalysisRule : flowAnalysisRules) {
            if (flowAnalysisRule.getState() == FlowAnalysisRuleState.DISABLED) {
                LOGGER.debug("Enabling FlowAnalysisRule with ID {}", flowAnalysisRule.getIdentifier());
                flowAnalysisRule.enable();
            }
        }

        for (final ProcessorNode processor : processors) {
            if (processor.getScheduledState() == ScheduledState.STOPPED) {
                LOGGER.debug("Starting Processor with ID {}", processor.getIdentifier());
                processor.getProcessGroup().startProcessor(processor, false);
            }
        }

        LOGGER.debug("Finished starting/enabling components that were stopped/disabled for reloading");
    }
}
