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

import static org.apache.nifi.web.api.dto.DtoFactory.SENSITIVE_VALUE_MASK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.FlowChangeRemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.StringUtils;
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

    // Proxy settings should be able to be null cleared, so the necessity of checking those properties depend on
    // whether transport protocol is specified.
    // See StandardRemoteProcessGroupDAO.updateRemoteProcessGroup for detail.
    private static final Function<RemoteProcessGroupDTO, Boolean> IS_TRANSPORT_PROTOCOL_SET = dto -> dto.getTransportProtocol() != null;

    private static final List<ConfigurationRecorder<RemoteProcessGroup, RemoteProcessGroupDTO>> CONFIG_RECORDERS = Arrays.asList(
            new ConfigurationRecorder<RemoteProcessGroup, RemoteProcessGroupDTO>("Communications Timeout",
                    dto -> dto.getCommunicationsTimeout() != null, RemoteProcessGroup::getCommunicationsTimeout),
            new ConfigurationRecorder<RemoteProcessGroup, RemoteProcessGroupDTO>("Yield Duration",
                    dto -> dto.getYieldDuration() != null, RemoteProcessGroup::getYieldDuration),
            new ConfigurationRecorder<RemoteProcessGroup, RemoteProcessGroupDTO>("Transport Protocol",
                    IS_TRANSPORT_PROTOCOL_SET, rpg -> rpg.getTransportProtocol().name()),
            new ConfigurationRecorder<>("Proxy Host",
                    IS_TRANSPORT_PROTOCOL_SET, RemoteProcessGroup::getProxyHost),
            new ConfigurationRecorder<>("Proxy Port",
                    IS_TRANSPORT_PROTOCOL_SET, RemoteProcessGroup::getProxyPort),
            new ConfigurationRecorder<>("Proxy User",
                    IS_TRANSPORT_PROTOCOL_SET, RemoteProcessGroup::getProxyUser),
            new ConfigurationRecorder<>("Proxy Password",
                    IS_TRANSPORT_PROTOCOL_SET, RemoteProcessGroup::getProxyPassword)
                    .setConvertRawValue(v -> StringUtils.isEmpty(v) ? "" : SENSITIVE_VALUE_MASK)
    );


    private static final BiFunction<RemoteGroupPort, String, String> PORT_NAME_CONVERT = (updated, name) -> updated.getName() + "." + name;
    private static final List<ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>> PORT_CONFIG_RECORDERS = Arrays.asList(
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Transmission",
                    dto -> dto.isTransmitting() != null, RemoteGroupPort::isRunning)
                    .setConvertName(PORT_NAME_CONVERT)
                    .setConvertRawValue(v -> Boolean.valueOf(v) ? "enabled" : "disabled"),
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Concurrent Tasks",
                    dto -> dto.getConcurrentlySchedulableTaskCount() != null, RemoteGroupPort::getMaxConcurrentTasks)
                    .setConvertName(PORT_NAME_CONVERT),
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Compressed",
                    dto -> dto.getUseCompression() != null, RemoteGroupPort::isUseCompression)
                    .setConvertName(PORT_NAME_CONVERT),
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Batch Count",
                    dto -> dto.getBatchSettings() != null && dto.getBatchSettings().getCount() != null, RemoteGroupPort::getBatchCount)
                    .setConvertName(PORT_NAME_CONVERT),
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Batch Size",
                    dto -> dto.getBatchSettings() != null && dto.getBatchSettings().getSize() != null, RemoteGroupPort::getBatchSize)
                    .setConvertName(PORT_NAME_CONVERT),
            new ConfigurationRecorder<RemoteGroupPort, RemoteProcessGroupPortDTO>("Batch Duration",
                    dto -> dto.getBatchSettings() != null && dto.getBatchSettings().getDuration() != null, RemoteGroupPort::getBatchDuration)
                    .setConvertName(PORT_NAME_CONVERT)
    );

    /**
     * Audits the creation of remote process groups via createRemoteProcessGroup().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return group
     * @throws java.lang.Throwable ex
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
     * Provides a higher order functions to compute Configuration audit events.
     * @param <CMP> Class of a component, such as RemoteProcessGroup or RemoteGroupPort
     * @param <DTO> Class of a DTO, such as RemoteProcessGroupDTO or RemoteProcessGroupPortDTO
     */
    private static class ConfigurationRecorder<CMP, DTO> {
        final String name;
        final Function<DTO, Boolean> hasInput;
        final Function<CMP, Object> getValue;
        private Function<String, String> convertRawValue;
        private BiFunction<CMP, String, String> convertName;

        /**
         * Create a recorder for a configuration property.
         * @param name name of the target property
         * @param hasInput a function that returns whether the property is being updated by a request
         * @param getValue a function that returns value of the property
         */
        private ConfigurationRecorder(String name, Function<DTO, Boolean> hasInput, Function<CMP, Object> getValue) {
            this.name = name;
            this.hasInput = hasInput;
            this.getValue = getValue;
        }

        private String convertRawValue(final String value) {
            return convertRawValue != null ? convertRawValue.apply(value) : value;
        }

        /**
         * If a property value needs to be converted for audit record, e.g. sensitive values,
         * use this method to specify a function to convert raw value.
         * @param convertRawValue a function to convert string representation of a property value
         * @return converted value
         */
        private ConfigurationRecorder<CMP, DTO> setConvertRawValue(final Function<String, String> convertRawValue) {
            this.convertRawValue = convertRawValue;
            return this;
        }

        /**
         * If a property name needs to be decorated depends on other values in a context,
         * use this method to specify a function to convert name.
         * @param convertName a function to convert name of a property
         * @return converted name
         */
        private ConfigurationRecorder<CMP, DTO> setConvertName(final BiFunction<CMP, String, String> convertName) {
            this.convertName = convertName;
            return this;
        }

        private ConfigureDetails checkConfigured(final DTO input,
                                                 final CMP updated,
                                                 final Object previousValue) {

            final Object updatedValue = getValue.apply(updated);
            // Convert null to empty String to avoid NullPointerException.
            final String updatedStr = updatedValue != null ? updatedValue.toString() : "";
            final String previousStr = previousValue != null ? previousValue.toString() : "";
            if (hasInput.apply(input) && !updatedStr.equals(previousStr)) {

                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName(convertName != null ? convertName.apply(updated, name) : name);
                configDetails.setPreviousValue(convertRawValue(previousStr));
                configDetails.setValue(convertRawValue(updatedStr));
                return configDetails;
            }
            return null;
        }

        /**
         * Capture values before a component to be updated. This method should be called before proceeding a joint point.
         * @param recorders list of ConfigurationRecorder
         * @param component a target component to capture
         * @param <C> Class of the target component
         * @param <D> DTO class of the target component
         * @return captured values keyed with its name
         */
        private static <C, D> Map<String, Object> capturePreviousValues(final List<ConfigurationRecorder<C, D>> recorders, final C component) {
            final Map<String, Object> previousValues = new HashMap<>(recorders.size());
            recorders.forEach(r -> previousValues.put(r.name, r.getValue.apply(component)));
            return previousValues;
        }

        /**
         * Generate ActionDetails for properties those have been updated.
         * This method should be called after proceeding a joint point with an updated component.
         * @param recorders list of ConfigurationRecorder
         * @param dto DTO instance containing requested values
         * @param updatedComponent a component instance that is updated by corresponding DAO
         * @param previousValues previous property values before being updated
         * @param details a Collection to accumulate action details generated
         * @param <C> Class of the target component
         * @param <D> DTO class of the target component
         */
        private static <C, D> void checkConfigured(final List<ConfigurationRecorder<C, D>> recorders, final D dto, final C updatedComponent,
                                                   final Map<String, Object> previousValues, final Collection<ActionDetails> details) {
            recorders.stream()
                    .map(r -> r.checkConfigured(dto, updatedComponent, previousValues.get(r.name)))
                    .filter(Objects::nonNull).forEach(d -> details.add(d));

        }
    }

    /**
     * Audits the update of remote process group configuration.
     *
     * @param proceedingJoinPoint   join point
     * @param remoteProcessGroupDTO dto
     * @param remoteProcessGroupDAO dao
     * @return group
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.RemoteProcessGroup updateRemoteProcessGroup(org.apache.nifi.web.api.dto.RemoteProcessGroupDTO)) && "
            + "args(remoteProcessGroupDTO) && "
            + "target(remoteProcessGroupDAO)")
    public RemoteProcessGroup auditUpdateProcessGroupConfiguration(
            ProceedingJoinPoint proceedingJoinPoint, RemoteProcessGroupDTO remoteProcessGroupDTO, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());

        // record the current value of this remoteProcessGroups configuration for comparisons later
        final boolean transmissionState = remoteProcessGroup.isTransmitting();
        final Map<String, Object> previousValues = ConfigurationRecorder.capturePreviousValues(CONFIG_RECORDERS, remoteProcessGroup);

        // perform the underlying operation
        final RemoteProcessGroup updatedRemoteProcessGroup = (RemoteProcessGroup) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            final Collection<ActionDetails> details = new ArrayList<>();

            // see if any property has changed
            ConfigurationRecorder.checkConfigured(CONFIG_RECORDERS, remoteProcessGroupDTO, updatedRemoteProcessGroup, previousValues, details);

            final Date timestamp = new Date();
            final Collection<Action> actions = new ArrayList<>();

            // create the remote process group details
            final FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = createFlowChangeDetails(remoteProcessGroup);

            // save the actions if necessary
            if (!details.isEmpty()) {

                // create the actions
                for (ActionDetails detail : details) {
                    // create a configure action for each updated property
                    FlowChangeAction remoteProcessGroupAction = createFlowChangeAction(user, timestamp,
                            updatedRemoteProcessGroup, remoteProcessGroupDetails);
                    remoteProcessGroupAction.setOperation(Operation.Configure);
                    remoteProcessGroupAction.setActionDetails(detail);

                    actions.add(remoteProcessGroupAction);
                }
            }

            // determine the new executing state
            boolean updatedTransmissionState = updatedRemoteProcessGroup.isTransmitting();

            // determine if the running state has changed
            if (transmissionState != updatedTransmissionState) {
                // create a remote process group action
                FlowChangeAction remoteProcessGroupAction = createFlowChangeAction(user, timestamp,
                        updatedRemoteProcessGroup, remoteProcessGroupDetails);

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

    private FlowChangeAction createFlowChangeAction(final NiFiUser user, final Date timestamp,
                                                    final RemoteProcessGroup remoteProcessGroup,
                                                    final FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails) {

        FlowChangeAction remoteProcessGroupAction = new FlowChangeAction();
        remoteProcessGroupAction.setUserIdentity(user.getIdentity());
        remoteProcessGroupAction.setTimestamp(timestamp);
        remoteProcessGroupAction.setSourceId(remoteProcessGroup.getIdentifier());
        remoteProcessGroupAction.setSourceName(remoteProcessGroup.getName());
        remoteProcessGroupAction.setSourceType(Component.RemoteProcessGroup);
        remoteProcessGroupAction.setComponentDetails(remoteProcessGroupDetails);
        return remoteProcessGroupAction;
    }

    /**
     * Audits the removal of a process group via deleteProcessGroup().
     *
     * @param proceedingJoinPoint join point
     * @param remoteProcessGroupId remote group id
     * @param remoteProcessGroupDAO remote group dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(void deleteRemoteProcessGroup(java.lang.String)) && "
            + "args(remoteProcessGroupId) && "
            + "target(remoteProcessGroupDAO)")
    public void removeRemoteProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, String remoteProcessGroupId, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {
        // get the remote process group before removing it
        RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);

        // remove the remote process group
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(remoteProcessGroup, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    private RemoteGroupPort auditUpdateProcessGroupPortConfiguration(ProceedingJoinPoint proceedingJoinPoint, RemoteProcessGroupPortDTO remoteProcessGroupPortDto,
                                                                     RemoteProcessGroup remoteProcessGroup, RemoteGroupPort remoteProcessGroupPort) throws Throwable {
        final Map<String, Object> previousValues = ConfigurationRecorder.capturePreviousValues(PORT_CONFIG_RECORDERS, remoteProcessGroupPort);

        // perform the underlying operation
        final RemoteGroupPort updatedRemoteProcessGroupPort = (RemoteGroupPort) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (user != null) {
            final Collection<ActionDetails> details = new ArrayList<>();

            // see if any property has changed
            ConfigurationRecorder.checkConfigured(PORT_CONFIG_RECORDERS, remoteProcessGroupPortDto, updatedRemoteProcessGroupPort, previousValues, details);

            final Date timestamp = new Date();
            final Collection<Action> actions = new ArrayList<>();

            // create the remote process group details
            final FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = createFlowChangeDetails(remoteProcessGroup);

            // save the actions if necessary
            for (ActionDetails detail : details) {
                // create a configure action for each updated property
                FlowChangeAction remoteProcessGroupAction = createFlowChangeAction(user, timestamp,
                        remoteProcessGroup, remoteProcessGroupDetails);
                remoteProcessGroupAction.setOperation(Operation.Configure);
                remoteProcessGroupAction.setActionDetails(detail);

                actions.add(remoteProcessGroupAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedRemoteProcessGroupPort;
    }

    /**
     * Audits the update of remote process group input port configuration.
     *
     * @param proceedingJoinPoint       join point
     * @param remoteProcessGroupPortDto dto
     * @param remoteProcessGroupDAO     dao
     * @return group
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(org.apache.nifi.remote.RemoteGroupPort updateRemoteProcessGroupInputPort(java.lang.String, org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO)) && "
            + "args(remoteProcessGroupId, remoteProcessGroupPortDto) && "
            + "target(remoteProcessGroupDAO)")
    public RemoteGroupPort auditUpdateProcessGroupInputPortConfiguration(
            ProceedingJoinPoint proceedingJoinPoint, String remoteProcessGroupId,
            RemoteProcessGroupPortDTO remoteProcessGroupPortDto, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort remoteProcessGroupPort = remoteProcessGroup.getInputPort(remoteProcessGroupPortDto.getId());

        return auditUpdateProcessGroupPortConfiguration(proceedingJoinPoint, remoteProcessGroupPortDto, remoteProcessGroup, remoteProcessGroupPort);
    }

    /**
     * Audits the update of remote process group output port configuration.
     *
     * @param proceedingJoinPoint join point
     * @param remoteProcessGroupPortDto dto
     * @param remoteProcessGroupDAO dao
     * @return group
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.RemoteProcessGroupDAO+) && "
            + "execution(org.apache.nifi.remote.RemoteGroupPort updateRemoteProcessGroupOutputPort(java.lang.String, org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO)) && "
            + "args(remoteProcessGroupId, remoteProcessGroupPortDto) && "
            + "target(remoteProcessGroupDAO)")
    public RemoteGroupPort auditUpdateProcessGroupOutputPortConfiguration(
            ProceedingJoinPoint proceedingJoinPoint, String remoteProcessGroupId,
            RemoteProcessGroupPortDTO remoteProcessGroupPortDto, RemoteProcessGroupDAO remoteProcessGroupDAO) throws Throwable {

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort remoteProcessGroupPort = remoteProcessGroup.getOutputPort(remoteProcessGroupPortDto.getId());

        return auditUpdateProcessGroupPortConfiguration(proceedingJoinPoint, remoteProcessGroupPortDto, remoteProcessGroup, remoteProcessGroupPort);
    }

    private FlowChangeRemoteProcessGroupDetails createFlowChangeDetails(RemoteProcessGroup remoteProcessGroup) {
        FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = new FlowChangeRemoteProcessGroupDetails();
        remoteProcessGroupDetails.setUri(remoteProcessGroup.getTargetUri());
        return remoteProcessGroupDetails;
    }


    /**
     * Generates an audit record for the specified remote process group.
     *
     * @param remoteProcessGroup group
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(RemoteProcessGroup remoteProcessGroup, Operation operation) {
        return generateAuditRecord(remoteProcessGroup, operation, null);
    }

    /**
     * Generates an audit record for the specified remote process group.
     *
     * @param remoteProcessGroup group
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(RemoteProcessGroup remoteProcessGroup, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the remote process group details
            FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = createFlowChangeDetails(remoteProcessGroup);

            // create the remote process group action
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
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
