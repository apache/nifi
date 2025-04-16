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

import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.web.controller.ControllerFacade;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Audits configuration changes to the controller.
 */
@Service
@Aspect
public class ControllerAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ControllerAuditor.class);

    /**
     * Audits updating the max number of timer driven threads for the controller.
     *
     * @param proceedingJoinPoint joint point
     * @param maxTimerDrivenThreadCount thread count
     * @param controllerFacade facade
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.controller.ControllerFacade) && "
            + "execution(void setMaxTimerDrivenThreadCount(int)) && "
            + "args(maxTimerDrivenThreadCount) && "
            + "target(controllerFacade)")
    public void updateControllerTimerDrivenThreadsAdvice(ProceedingJoinPoint proceedingJoinPoint, int maxTimerDrivenThreadCount, ControllerFacade controllerFacade) throws Throwable {
        // get the current max thread count
        int previousMaxTimerDrivenThreadCount = controllerFacade.getMaxTimerDrivenThreadCount();

        // update the processors state
        proceedingJoinPoint.proceed();

        // if no exception were thrown, add the configuration action...
        // ensure the value changed
        if (previousMaxTimerDrivenThreadCount != maxTimerDrivenThreadCount) {
            if (isAuditable()) {
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Controller Max Timer Driven Thread Count");
                configDetails.setValue(String.valueOf(maxTimerDrivenThreadCount));
                configDetails.setPreviousValue(String.valueOf(previousMaxTimerDrivenThreadCount));

                // create the config action
                FlowChangeAction configAction = createFlowChangeAction();
                configAction.setOperation(Operation.Configure);
                configAction.setSourceId("Flow Controller");
                configAction.setSourceName("Flow Controller");
                configAction.setSourceType(Component.Controller);
                configAction.setActionDetails(configDetails);

                saveAction(configAction, logger);
            }
        }
    }
}
