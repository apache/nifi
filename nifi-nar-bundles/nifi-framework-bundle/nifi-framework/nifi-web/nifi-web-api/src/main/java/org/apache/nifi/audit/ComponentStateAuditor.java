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
package org.apache.nifi.audit;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.authorization.user.NiFiUser;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

@Aspect
public class ComponentStateAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ComponentStateAuditor.class);

    /**
     * Audits clearing of state from a Processor.
     *
     * @param proceedingJoinPoint join point
     * @param processor the processor
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ComponentStateDAO+) && "
        + "execution(void clearState(org.apache.nifi.controller.ProcessorNode)) && "
        + "args(processor)")
    public StateMap clearProcessorStateAdvice(ProceedingJoinPoint proceedingJoinPoint, ProcessorNode processor) throws Throwable {

        // update the processors state
        final StateMap stateMap = (StateMap) proceedingJoinPoint.proceed();

        // if no exception were thrown, add the clear action...

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<Action> actions = new ArrayList<>();

            // create the processor details
            FlowChangeExtensionDetails processorDetails = new FlowChangeExtensionDetails();
            processorDetails.setType(processor.getComponentType());

            // create the clear action
            FlowChangeAction configAction = new FlowChangeAction();
            configAction.setUserIdentity(user.getIdentity());
            configAction.setOperation(Operation.ClearState);
            configAction.setTimestamp(new Date());
            configAction.setSourceId(processor.getIdentifier());
            configAction.setSourceName(processor.getName());
            configAction.setSourceType(Component.Processor);
            configAction.setComponentDetails(processorDetails);
            actions.add(configAction);

            // record the action
            saveActions(actions, logger);
        }

        return stateMap;
    }

    /**
     * Audits clearing of state from a Controller Service.
     *
     * @param proceedingJoinPoint join point
     * @param controllerService the controller service
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ComponentStateDAO+) && "
        + "execution(void clearState(org.apache.nifi.controller.service.ControllerServiceNode)) && "
        + "args(controllerService)")
    public StateMap clearControllerServiceStateAdvice(ProceedingJoinPoint proceedingJoinPoint, ControllerServiceNode controllerService) throws Throwable {

        // update the controller service state
        final StateMap stateMap = (StateMap) proceedingJoinPoint.proceed();

        // if no exception were thrown, add the clear action...

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<Action> actions = new ArrayList<>();

            // create the controller service details
            FlowChangeExtensionDetails controllerServiceDetails = new FlowChangeExtensionDetails();
            controllerServiceDetails.setType(controllerService.getComponentType());

            // create the clear action
            FlowChangeAction configAction = new FlowChangeAction();
            configAction.setUserIdentity(user.getIdentity());
            configAction.setOperation(Operation.ClearState);
            configAction.setTimestamp(new Date());
            configAction.setSourceId(controllerService.getIdentifier());
            configAction.setSourceName(controllerService.getName());
            configAction.setSourceType(Component.ControllerService);
            configAction.setComponentDetails(controllerServiceDetails);
            actions.add(configAction);

            // record the action
            saveActions(actions, logger);
        }

        return stateMap;
    }

    /**
     * Audits clearing of state from a Processor.
     *
     * @param proceedingJoinPoint join point
     * @param reportingTask the reporting task
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ComponentStateDAO+) && "
        + "execution(void clearState(org.apache.nifi.controller.ReportingTaskNode)) && "
        + "args(reportingTask)")
    public StateMap clearReportingTaskStateAdvice(ProceedingJoinPoint proceedingJoinPoint, ReportingTaskNode reportingTask) throws Throwable {

        // update the reporting task state
        final StateMap stateMap = (StateMap) proceedingJoinPoint.proceed();

        // if no exception were thrown, add the clear action...

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<Action> actions = new ArrayList<>();

            // create the reporting task details
            FlowChangeExtensionDetails reportingTaskDetails = new FlowChangeExtensionDetails();
            reportingTaskDetails.setType(reportingTask.getReportingTask().getClass().getSimpleName());

            // create the clear action
            FlowChangeAction configAction = new FlowChangeAction();
            configAction.setUserIdentity(user.getIdentity());
            configAction.setOperation(Operation.ClearState);
            configAction.setTimestamp(new Date());
            configAction.setSourceId(reportingTask.getIdentifier());
            configAction.setSourceName(reportingTask.getName());
            configAction.setSourceType(Component.ReportingTask);
            configAction.setComponentDetails(reportingTaskDetails);
            actions.add(configAction);

            // record the action
            saveActions(actions, logger);
        }

        return stateMap;
    }
}
