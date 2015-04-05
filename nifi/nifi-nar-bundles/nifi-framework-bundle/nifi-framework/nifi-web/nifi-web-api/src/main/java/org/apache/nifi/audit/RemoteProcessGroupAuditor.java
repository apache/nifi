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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.RemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audits remote process group creation/removal and configuration changes.
 */
@Aspect
public class RemoteProcessGroupAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessGroupAuditor.class);

    /**
     * Audits the creation of remote process groups via
     * createRemoteProcessGroup().
     *
     * This method only needs to be run 'after returning'. However, in Java 7
     * the order in which these methods are returned from
     * Class.getDeclaredMethods (even though there is no order guaranteed) seems
     * to differ from Java 6. SpringAOP depends on this ordering to determine
     * advice precedence. By normalizing all advice into Around advice we can
     * alleviate this issue.
     *
     * @param proceedingJoinPoint
     * @return 
     * @throws java.lang.Throwable 
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.RemoteProcessGroup createRemoteProcessGroup(java.lang.String, org.apache.nifi.web.api.dto.RemoteProcessGroupDTO))")
    public RemoteProcessGroup createRemoteProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the remote process group
        RemoteProcessGroup remoteProcessGroup = (RemoteProcessGroup) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the remote process group action...
        // get the creation audits
        final Action action = generateAuditRecord(remoteProcessGroup, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return remoteProcessGroup;
    }

    /**
     * Audits the update of remote process group configuration.
     *
     * @param proceedingJoinPoint
     * @param groupId
     * @param remoteProcessGroupDTO
     * @param remoteProcessGroupDAO
     * @return
     * @throws Throwable
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.RemoteProcessGroup updateRemoteProcessGroup(java.lang.String, org.apache.nifi.web.api.dto.RemoteProcessGroupDTO)) && "
            + "args(groupId, remoteProcessGroupDTO) && "
            + "target(remoteProcessGroupDAO)")
    public RemoteProcessGroup auditUpdateProcessGroupConfiguration(ProceedingJoinPoint proceedingJoinPoint, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(groupId, remoteProcessGroupDTO.getId());

        // record the current value of this remoteProcessGroups configuration for comparisons later
        final boolean transmissionState = remoteProcessGroup.isTransmitting();
        final String communicationsTimeout = remoteProcessGroup.getCommunicationsTimeout();
        final String yieldDuration = remoteProcessGroup.getYieldDuration();
        final Map<String, Integer> concurrentTasks = new HashMap<>();
        final Map<String, Boolean> compression = new HashMap<>();
        for (final RemoteGroupPort remotePort : remoteProcessGroup.getInputPorts()) {
            concurrentTasks.put(remotePort.getIdentifier(), remotePort.getMaxConcurrentTasks());
            compression.put(remotePort.getIdentifier(), remotePort.isUseCompression());
        }
        for (final RemoteGroupPort remotePort : remoteProcessGroup.getOutputPorts()) {
            concurrentTasks.put(remotePort.getIdentifier(), remotePort.getMaxConcurrentTasks());
            compression.put(remotePort.getIdentifier(), remotePort.isUseCompression());
        }

        // perform the underlying operation
        final RemoteProcessGroup updatedRemoteProcessGroup = (RemoteProcessGroup) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<ActionDetails> details = new ArrayList<>();

            // see if the communications timeout has changed
            if (remoteProcessGroupDTO.getCommunicationsTimeout() != null && !updatedRemoteProcessGroup.getCommunicationsTimeout().equals(communicationsTimeout)) {
                // create the config details
                ConfigureDetails configDetails = new ConfigureDetails();
                configDetails.setName("Communications Timeout");
                configDetails.setValue(updatedRemoteProcessGroup.getCommunicationsTimeout());
                configDetails.setPreviousValue(communicationsTimeout);

                details.add(configDetails);
            }

            // see if the yield duration has changed
            if (remoteProcessGroupDTO.getYieldDuration() != null && !updatedRemoteProcessGroup.getYieldDuration().equals(yieldDuration)) {
                // create the config details
                ConfigureDetails configDetails = new ConfigureDetails();
                configDetails.setName("Yield Duration");
                configDetails.setValue(updatedRemoteProcessGroup.getYieldDuration());
                configDetails.setPreviousValue(yieldDuration);

                details.add(configDetails);
            }

            // see if the contents of this remote process group are possibly changing
            if (remoteProcessGroupDTO.getContents() != null) {
                final RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();

                // see if any input port configuration is changing
                if (contents.getInputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePortDTO : contents.getInputPorts()) {
                        final RemoteGroupPort remotePort = updatedRemoteProcessGroup.getInputPort(remotePortDTO.getId());

                        // if this port has been removed, ignore the configuration change for auditing purposes
                        if (remotePort == null) {
                            continue;
                        }

                        // if a new concurrent task count is specified
                        if (remotePortDTO.getConcurrentlySchedulableTaskCount() != null) {
                            // see if the concurrent tasks has changed
                            final Integer previousConcurrentTasks = concurrentTasks.get(remotePortDTO.getId());
                            if (previousConcurrentTasks != null && remotePort.getMaxConcurrentTasks() != previousConcurrentTasks) {
                                // create the config details
                                ConfigureDetails concurrentTasksDetails = new ConfigureDetails();
                                concurrentTasksDetails.setName("Concurrent Tasks");
                                concurrentTasksDetails.setValue(String.valueOf(remotePort.getMaxConcurrentTasks()));
                                concurrentTasksDetails.setPreviousValue(String.valueOf(previousConcurrentTasks));

                                details.add(concurrentTasksDetails);
                            }
                        }

                        // if a new compressed flag is specified
                        if (remotePortDTO.getUseCompression() != null) {
                            // see if the compression has changed
                            final Boolean previousCompression = compression.get(remotePortDTO.getId());
                            if (previousCompression != null && remotePort.isUseCompression() != previousCompression) {
                                // create the config details
                                ConfigureDetails compressionDetails = new ConfigureDetails();
                                compressionDetails.setName("Compressed");
                                compressionDetails.setValue(String.valueOf(remotePort.isUseCompression()));
                                compressionDetails.setPreviousValue(String.valueOf(previousCompression));

                                details.add(compressionDetails);
                            }
                        }
                    }
                }

                // see if any output port configuration is changing
                if (contents.getOutputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePortDTO : contents.getOutputPorts()) {
                        final RemoteGroupPort remotePort = updatedRemoteProcessGroup.getOutputPort(remotePortDTO.getId());

                        // if this port has been removed, ignore the configuration change for auditing purposes
                        if (remotePort == null) {
                            continue;
                        }

                        // if a new concurrent task count is specified
                        if (remotePortDTO.getConcurrentlySchedulableTaskCount() != null) {
                            // see if the concurrent tasks has changed
                            final Integer previousConcurrentTasks = concurrentTasks.get(remotePortDTO.getId());
                            if (previousConcurrentTasks != null && remotePort.getMaxConcurrentTasks() != previousConcurrentTasks) {
                                // create the config details
                                ConfigureDetails concurrentTasksDetails = new ConfigureDetails();
                                concurrentTasksDetails.setName("Concurrent Tasks");
                                concurrentTasksDetails.setValue(String.valueOf(remotePort.getMaxConcurrentTasks()));
                                concurrentTasksDetails.setPreviousValue(String.valueOf(previousConcurrentTasks));

                                details.add(concurrentTasksDetails);
                            }
                        }

                        // if a new compressed flag is specified
                        if (remotePortDTO.getUseCompression() != null) {
                            // see if the compression has changed
                            final Boolean previousCompression = compression.get(remotePortDTO.getId());
                            if (previousCompression != null && remotePort.isUseCompression() != previousCompression) {
                                // create the config details
                                ConfigureDetails compressionDetails = new ConfigureDetails();
                                compressionDetails.setName("Compressed");
                                compressionDetails.setValue(String.valueOf(remotePort.isUseCompression()));
                                compressionDetails.setPreviousValue(String.valueOf(previousCompression));

                                details.add(compressionDetails);
                            }
                        }
                    }
                }
            }

            Collection<Action> actions = new ArrayList<>();

            // create the remote process group details
            RemoteProcessGroupDetails remoteProcessGroupDetails = new RemoteProcessGroupDetails();
            remoteProcessGroupDetails.setUri(remoteProcessGroup.getTargetUri().toString());

            // save the actions if necessary
            if (!details.isEmpty()) {
                Date timestamp = new Date();

                // create the actions
                for (ActionDetails detail : details) {
                    // create the port action for updating the name
                    Action remoteProcessGroupAction = new Action();
                    remoteProcessGroupAction.setUserDn(user.getDn());
                    remoteProcessGroupAction.setUserName(user.getUserName());
                    remoteProcessGroupAction.setOperation(Operation.Configure);
                    remoteProcessGroupAction.setTimestamp(timestamp);
                    remoteProcessGroupAction.setSourceId(updatedRemoteProcessGroup.getIdentifier());
                    remoteProcessGroupAction.setSourceName(updatedRemoteProcessGroup.getName());
                    remoteProcessGroupAction.setSourceType(Component.RemoteProcessGroup);
                    remoteProcessGroupAction.setComponentDetails(remoteProcessGroupDetails);
                    remoteProcessGroupAction.setActionDetails(detail);

                    actions.add(remoteProcessGroupAction);
                }
            }

            // determine the new executing state
            boolean updatedTransmissionState = updatedRemoteProcessGroup.isTransmitting();

            // determine if the running state has changed
            if (transmissionState != updatedTransmissionState) {
                // create a processor action
                Action remoteProcessGroupAction = new Action();
                remoteProcessGroupAction.setUserDn(user.getDn());
                remoteProcessGroupAction.setUserName(user.getUserName());
                remoteProcessGroupAction.setTimestamp(new Date());
                remoteProcessGroupAction.setSourceId(updatedRemoteProcessGroup.getIdentifier());
                remoteProcessGroupAction.setSourceName(updatedRemoteProcessGroup.getName());
                remoteProcessGroupAction.setSourceType(Component.RemoteProcessGroup);
                remoteProcessGroupAction.setComponentDetails(remoteProcessGroupDetails);

                // set the operation accordingly
                if (updatedTransmissionState) {
                    remoteProcessGroupAction.setOperation(Operation.Start);
                } else {
                    remoteProcessGroupAction.setOperation(Operation.Stop);
                }
                actions.add(remoteProcessGroupAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedRemoteProcessGroup;
    }

    /**
     * Audits the removal of a process group via deleteProcessGroup().
     *
     * @param proceedingJoinPoint
     * @param groupId
     * @param remoteProcessGroupId
     * @param remoteProcessGroupDAO
     * @throws Throwable
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(void deleteRemoteProcessGroup(java.lang.String, java.lang.String)) && "
            + "args(groupId, remoteProcessGroupId) && "
            + "target(remoteProcessGroupDAO)")
    public void removeRemoteProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, String remoteProcessGroupId, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {
        // get the remote process group before removing it
        RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(groupId, remoteProcessGroupId);

        // remove the remote process group
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(remoteProcessGroup, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the specified remote process group.
     *
     * @param remoteProcessGroup
     * @param operation
     * @return 
     */
    public Action generateAuditRecord(RemoteProcessGroup remoteProcessGroup, Operation operation) {
        return generateAuditRecord(remoteProcessGroup, operation, null);
    }

    /**
     * Generates an audit record for the specified remote process group.
     *
     * @param remoteProcessGroup
     * @param operation
     * @param actionDetails
     * @return 
     */
    public Action generateAuditRecord(RemoteProcessGroup remoteProcessGroup, Operation operation, ActionDetails actionDetails) {
        Action action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the remote process group details
            RemoteProcessGroupDetails remoteProcessGroupDetails = new RemoteProcessGroupDetails();
            remoteProcessGroupDetails.setUri(remoteProcessGroup.getTargetUri().toString());

            // create the remote process group action
            action = new Action();
            action.setUserDn(user.getDn());
            action.setUserName(user.getUserName());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(remoteProcessGroup.getIdentifier());
            action.setSourceName(remoteProcessGroup.getName());
            action.setSourceType(Component.RemoteProcessGroup);
            action.setComponentDetails(remoteProcessGroupDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
