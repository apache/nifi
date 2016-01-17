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
package org.apache.nifi.web.dao.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;

import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskProvider;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.quartz.CronExpression;

public class StandardReportingTaskDAO extends ComponentDAO implements ReportingTaskDAO {

    private ReportingTaskProvider reportingTaskProvider;
    private ComponentStateDAO componentStateDAO;

    private ReportingTaskNode locateReportingTask(final String reportingTaskId) {
        // get the reporting task
        final ReportingTaskNode reportingTask = reportingTaskProvider.getReportingTaskNode(reportingTaskId);

        // ensure the reporting task exists
        if (reportingTask == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate reporting task with id '%s'.", reportingTaskId));
        }

        return reportingTask;
    }

    @Override
    public ReportingTaskNode createReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        // ensure the type is specified
        if (reportingTaskDTO.getType() == null) {
            throw new IllegalArgumentException("The reporting task type must be specified.");
        }

        try {
            // create the reporting task
            final ReportingTaskNode reportingTask = reportingTaskProvider.createReportingTask(reportingTaskDTO.getType(), reportingTaskDTO.getId(), true);

            // ensure we can perform the update
            verifyUpdate(reportingTask, reportingTaskDTO);

            // perform the update
            configureReportingTask(reportingTask, reportingTaskDTO);

            return reportingTask;
        } catch (ReportingTaskInstantiationException rtie) {
            throw new NiFiCoreException(rtie.getMessage(), rtie);
        }
    }

    @Override
    public ReportingTaskNode getReportingTask(final String reportingTaskId) {
        return locateReportingTask(reportingTaskId);
    }

    @Override
    public boolean hasReportingTask(final String reportingTaskId) {
        return reportingTaskProvider.getReportingTaskNode(reportingTaskId) != null;
    }

    @Override
    public Set<ReportingTaskNode> getReportingTasks() {
        return reportingTaskProvider.getAllReportingTasks();
    }

    @Override
    public ReportingTaskNode updateReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        // get the reporting task
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskDTO.getId());

        // ensure we can perform the update
        verifyUpdate(reportingTask, reportingTaskDTO);

        // perform the update
        configureReportingTask(reportingTask, reportingTaskDTO);

        // configure scheduled state
        // see if an update is necessary
        if (isNotNull(reportingTaskDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(reportingTaskDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(reportingTask.getScheduledState())) {
                try {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            reportingTaskProvider.startReportingTask(reportingTask);
                            break;
                        case STOPPED:
                            switch (reportingTask.getScheduledState()) {
                                case RUNNING:
                                    reportingTaskProvider.stopReportingTask(reportingTask);
                                    break;
                                case DISABLED:
                                    reportingTaskProvider.enableReportingTask(reportingTask);
                                    break;
                            }
                            break;
                        case DISABLED:
                            reportingTaskProvider.disableReportingTask(reportingTask);
                            break;
                    }
                } catch (IllegalStateException | ComponentLifeCycleException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                } catch (RejectedExecutionException ree) {
                    throw new NiFiCoreException("Unable to schedule all tasks for the specified reporting task.", ree);
                } catch (NullPointerException npe) {
                    throw new NiFiCoreException("Unable to update reporting task run state.", npe);
                } catch (Exception e) {
                    throw new NiFiCoreException("Unable to update reporting task run state: " + e, e);
                }
            }
        }

        return reportingTask;
    }

    private List<String> validateProposedConfiguration(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        final List<String> validationErrors = new ArrayList<>();

        // get the current scheduling strategy
        SchedulingStrategy schedulingStrategy = reportingTask.getSchedulingStrategy();

        // validate the new scheduling strategy if appropriate
        if (isNotNull(reportingTaskDTO.getSchedulingStrategy())) {
            try {
                // this will be the new scheduling strategy so use it
                schedulingStrategy = SchedulingStrategy.valueOf(reportingTaskDTO.getSchedulingStrategy());
            } catch (IllegalArgumentException iae) {
                validationErrors.add(String.format("Scheduling strategy: Value must be one of [%s]", StringUtils.join(SchedulingStrategy.values(), ", ")));
            }
        }

        // validate the scheduling period based on the scheduling strategy
        if (isNotNull(reportingTaskDTO.getSchedulingPeriod())) {
            switch (schedulingStrategy) {
                case TIMER_DRIVEN:
                    final Matcher schedulingMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(reportingTaskDTO.getSchedulingPeriod());
                    if (!schedulingMatcher.matches()) {
                        validationErrors.add("Scheduling period is not a valid time duration (ie 30 sec, 5 min)");
                    }
                    break;
                case CRON_DRIVEN:
                    try {
                        new CronExpression(reportingTaskDTO.getSchedulingPeriod());
                    } catch (final ParseException pe) {
                        throw new IllegalArgumentException(String.format("Scheduling Period '%s' is not a valid cron expression: %s", reportingTaskDTO.getSchedulingPeriod(), pe.getMessage()));
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Scheduling Period is not a valid cron expression: " + reportingTaskDTO.getSchedulingPeriod());
                    }
                    break;
            }
        }

        return validationErrors;
    }

    @Override
    public void verifyDelete(final String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        reportingTask.verifyCanDelete();
    }

    @Override
    public void verifyUpdate(final ReportingTaskDTO reportingTaskDTO) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskDTO.getId());
        verifyUpdate(reportingTask, reportingTaskDTO);
    }

    private void verifyUpdate(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        // ensure the state, if specified, is valid
        if (isNotNull(reportingTaskDTO.getState())) {
            try {
                final ScheduledState purposedScheduledState = ScheduledState.valueOf(reportingTaskDTO.getState());

                // only attempt an action if it is changing
                if (!purposedScheduledState.equals(reportingTask.getScheduledState())) {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            reportingTask.verifyCanStart();
                            break;
                        case STOPPED:
                            switch (reportingTask.getScheduledState()) {
                                case RUNNING:
                                    reportingTask.verifyCanStop();
                                    break;
                                case DISABLED:
                                    reportingTask.verifyCanEnable();
                                    break;
                            }
                            break;
                        case DISABLED:
                            reportingTask.verifyCanDisable();
                            break;
                    }
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(String.format(
                        "The specified reporting task state (%s) is not valid. Valid options are 'RUNNING', 'STOPPED', and 'DISABLED'.",
                        reportingTaskDTO.getState()));
            }
        }

        boolean modificationRequest = false;
        if (isAnyNotNull(reportingTaskDTO.getName(),
                reportingTaskDTO.getSchedulingStrategy(),
                reportingTaskDTO.getSchedulingPeriod(),
                reportingTaskDTO.getAnnotationData(),
                reportingTaskDTO.getProperties())) {
            modificationRequest = true;

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(reportingTask, reportingTaskDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }

        if (modificationRequest) {
            reportingTask.verifyCanUpdate();
        }
    }

    private void configureReportingTask(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        final String name = reportingTaskDTO.getName();
        final String schedulingStrategy = reportingTaskDTO.getSchedulingStrategy();
        final String schedulingPeriod = reportingTaskDTO.getSchedulingPeriod();
        final String annotationData = reportingTaskDTO.getAnnotationData();
        final String comments = reportingTaskDTO.getComments();
        final Map<String, String> properties = reportingTaskDTO.getProperties();

        // ensure scheduling strategy is set first
        if (isNotNull(schedulingStrategy)) {
            reportingTask.setSchedulingStrategy(SchedulingStrategy.valueOf(schedulingStrategy));
        }

        if (isNotNull(name)) {
            reportingTask.setName(name);
        }
        if (isNotNull(schedulingPeriod)) {
            reportingTask.setScheduldingPeriod(schedulingPeriod);
        }
        if (isNotNull(annotationData)) {
            reportingTask.setAnnotationData(annotationData);
        }
        if (isNotNull(comments)) {
            reportingTask.setComments(comments);
        }
        if (isNotNull(properties)) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String propName = entry.getKey();
                final String propVal = entry.getValue();
                if (isNotNull(propName) && propVal == null) {
                    reportingTask.removeProperty(propName);
                } else if (isNotNull(propName)) {
                    reportingTask.setProperty(propName, propVal);
                }
            }
        }
    }

    @Override
    public void deleteReportingTask(String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        reportingTaskProvider.removeReportingTask(reportingTask);
    }

    @Override
    public StateMap getState(String reportingTaskId, Scope scope) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        return componentStateDAO.getState(reportingTask, scope);
    }

    @Override
    public void verifyClearState(String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        reportingTask.verifyCanClearState();
    }

    @Override
    public void clearState(String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        componentStateDAO.clearState(reportingTask);
    }

    /* setters */
    public void setReportingTaskProvider(ReportingTaskProvider reportingTaskProvider) {
        this.reportingTaskProvider = reportingTaskProvider;
    }

    public void setComponentStateDAO(ComponentStateDAO componentStateDAO) {
        this.componentStateDAO = componentStateDAO;
    }
}
