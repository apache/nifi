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
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.RemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.web.api.dto.BatchSettingsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.web.api.dto.DtoFactory.SENSITIVE_VALUE_MASK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRemoteProcessGroupAuditor {

    @Before
    public void setup() {

        final SecurityContext securityContext = SecurityContextHolder.getContext();
        final Authentication authentication = mock(Authentication.class);
        securityContext.setAuthentication(authentication);
        final NiFiUser user = new Builder().identity("user-id").build();
        final NiFiUserDetails userDetail = new NiFiUserDetails(user);
        when(authentication.getPrincipal()).thenReturn(userDetail);

    }

    @SuppressWarnings("unchecked")
    private Collection<Action> updateProcessGroupConfiguration(RemoteProcessGroupDTO inputRPGDTO, RemoteProcessGroup existingRPG) throws Throwable {
        final RemoteProcessGroupAuditor auditor = new RemoteProcessGroupAuditor();
        final ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        final String remoteProcessGroupId = "remote-process-group-id";
        inputRPGDTO.setId(remoteProcessGroupId);

        final String targetUrl = "http://localhost:8080/nifi";
        when(existingRPG.getTargetUri()).thenReturn(targetUrl);

        final RemoteProcessGroupDAO remoteProcessGroupDAO = mock(RemoteProcessGroupDAO.class);
        when(remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId))
                .thenReturn(existingRPG);

        // Setup updatedRPG mock based on inputRPGDTO.
        final RemoteProcessGroup updatedRPG = mock(RemoteProcessGroup.class);
        when(updatedRPG.getIdentifier()).thenReturn(remoteProcessGroupId);
        when(updatedRPG.isTransmitting()).thenReturn(inputRPGDTO.isTransmitting());
        when(updatedRPG.getCommunicationsTimeout()).thenReturn(inputRPGDTO.getCommunicationsTimeout());
        when(updatedRPG.getYieldDuration()).thenReturn(inputRPGDTO.getYieldDuration());
        when(updatedRPG.getTransportProtocol())
                .thenReturn(SiteToSiteTransportProtocol.valueOf(inputRPGDTO.getTransportProtocol()));
        when(updatedRPG.getProxyHost()).thenReturn(inputRPGDTO.getProxyHost());
        when(updatedRPG.getProxyPort()).thenReturn(inputRPGDTO.getProxyPort());
        when(updatedRPG.getProxyUser()).thenReturn(inputRPGDTO.getProxyUser());
        when(updatedRPG.getProxyPassword()).thenReturn(inputRPGDTO.getProxyPassword());

        when(joinPoint.proceed()).thenReturn(updatedRPG);

        // Capture added actions so that those can be asserted later.
        final AuditService auditService = mock(AuditService.class);
        final AtomicReference<Collection<Action>> addedActions = new AtomicReference<>();
        doAnswer(invocation -> {
            Collection<Action> actions = invocation.getArgumentAt(0, Collection.class);
            addedActions.set(actions);
            return null;
        }).when(auditService).addActions(any());

        auditor.setAuditService(auditService);

        auditor.auditUpdateProcessGroupConfiguration(joinPoint, inputRPGDTO, remoteProcessGroupDAO);


        final Collection<Action> actions = addedActions.get();

        // Assert common action values.
        if (actions != null) {
            actions.forEach(action -> {
                assertEquals(remoteProcessGroupId, action.getSourceId());
                assertEquals("user-id", action.getUserIdentity());
                assertEquals(targetUrl, ((RemoteProcessGroupDetails)action.getComponentDetails()).getUri());
                assertNotNull(action.getTimestamp());
            });
        }

        return actions;
    }

    private RemoteProcessGroup defaultRemoteProcessGroup() {
        final RemoteProcessGroup existingRPG = mock(RemoteProcessGroup.class);
        when(existingRPG.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.RAW);
        when(existingRPG.isTransmitting()).thenReturn(false);
        when(existingRPG.getProxyPort()).thenReturn(null);
        return existingRPG;
    }

    private RemoteProcessGroupDTO defaultInput() {
        final RemoteProcessGroupDTO inputRPGDTO = new RemoteProcessGroupDTO();
        inputRPGDTO.setTransportProtocol("RAW");
        inputRPGDTO.setTransmitting(false);
        return inputRPGDTO;
    }

    @Test
    public void testEnableTransmission() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.isTransmitting()).thenReturn(false);

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setTransmitting(true);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Start, action.getOperation());
        assertNull(action.getActionDetails());

    }

    @Test
    public void testDisableTransmission() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.isTransmitting()).thenReturn(true);

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setTransmitting(false);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Stop, action.getOperation());
        assertNull(action.getActionDetails());

    }

    @Test
    public void testConfigureCommunicationsTimeout() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getCommunicationsTimeout()).thenReturn("30 sec");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setCommunicationsTimeout("31 sec");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Communications Timeout",
                existingRPG.getCommunicationsTimeout(), inputRPGDTO.getCommunicationsTimeout());

    }

    private void assertConfigureDetails(final ActionDetails actionDetails, final String name,
                                        final Object previousValue, final Object value) {
        final ConfigureDetails configureDetails = (ConfigureDetails) actionDetails;
        assertEquals(name, configureDetails.getName());
        assertEquals(previousValue != null ? previousValue.toString() : "", configureDetails.getPreviousValue());
        assertEquals(value != null ? value.toString() : "", configureDetails.getValue());
    }

    @Test
    public void testConfigureYieldDuration() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getYieldDuration()).thenReturn("10 sec");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setYieldDuration("11 sec");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Yield Duration",
                existingRPG.getYieldDuration(), inputRPGDTO.getYieldDuration());

    }

    @Test
    public void testConfigureTransportProtocol() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.RAW);

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setTransportProtocol("HTTP");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Transport Protocol",
                existingRPG.getTransportProtocol().name(), inputRPGDTO.getTransportProtocol());

    }

    @Test
    public void testConfigureProxyHost() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyHost("proxy.example.com");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Host",
                existingRPG.getProxyHost(), inputRPGDTO.getProxyHost());

    }

    @Test
    public void testConfigureProxyHostUpdate() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getProxyHost()).thenReturn("proxy1.example.com");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyHost("proxy2.example.com");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Host",
                existingRPG.getProxyHost(), inputRPGDTO.getProxyHost());

    }

    @Test
    public void testConfigureProxyHostClear() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getProxyHost()).thenReturn("proxy.example.com");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyHost("");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Host",
                existingRPG.getProxyHost(), inputRPGDTO.getProxyHost());

    }

    @Test
    public void testConfigureProxyPort() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyPort(3128);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Port",
                existingRPG.getProxyPort(), inputRPGDTO.getProxyPort());

    }


    @Test
    public void testConfigureProxyPortClear() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getProxyPort()).thenReturn(3128);

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyPort(null);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Port",
                existingRPG.getProxyPort(), inputRPGDTO.getProxyPort());

    }

    @Test
    public void testConfigureProxyUser() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyUser("proxy-user");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy User",
                existingRPG.getProxyUser(), inputRPGDTO.getProxyUser());

    }

    @Test
    public void testConfigureProxyUserClear() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getProxyUser()).thenReturn("proxy-user");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyUser(null);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy User",
                existingRPG.getProxyUser(), inputRPGDTO.getProxyUser());

    }

    @Test
    public void testConfigureProxyPassword() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyPassword("proxy-password");

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Password","", SENSITIVE_VALUE_MASK);

    }

    @Test
    public void testConfigureProxyPasswordClear() throws Throwable {

        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        when(existingRPG.getProxyPassword()).thenReturn("proxy-password");

        final RemoteProcessGroupDTO inputRPGDTO = defaultInput();
        inputRPGDTO.setProxyPassword(null);

        final Collection<Action> actions = updateProcessGroupConfiguration(inputRPGDTO, existingRPG);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "Proxy Password", SENSITIVE_VALUE_MASK, "");

    }

    @SuppressWarnings("unchecked")
    private Collection<Action> updateProcessGroupInputPortConfiguration(RemoteProcessGroupPortDTO inputRPGPortDTO, RemoteGroupPort existingRPGPort) throws Throwable {
        final RemoteProcessGroup existingRPG = defaultRemoteProcessGroup();
        final RemoteProcessGroupAuditor auditor = new RemoteProcessGroupAuditor();
        final ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        final String remoteProcessGroupId = "remote-process-group-id";
        inputRPGPortDTO.setId(remoteProcessGroupId);

        final String targetUrl = "http://localhost:8080/nifi";
        when(existingRPG.getIdentifier()).thenReturn(remoteProcessGroupId);
        when(existingRPG.getTargetUri()).thenReturn(targetUrl);

        final RemoteProcessGroupDAO remoteProcessGroupDAO = mock(RemoteProcessGroupDAO.class);
        when(remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId))
                .thenReturn(existingRPG);

        when(existingRPG.getInputPort(eq(inputRPGPortDTO.getId()))).thenReturn(existingRPGPort);

        // Setup updatedRPGPort mock based on inputRPGPortDTO.
        final RemoteGroupPort updatedRPGPort = mock(RemoteGroupPort.class);
        final String portName = existingRPGPort.getName();
        when(updatedRPGPort.getName()).thenReturn(portName);
        if (inputRPGPortDTO.isTransmitting() != null) {
            when(updatedRPGPort.isRunning()).thenReturn(inputRPGPortDTO.isTransmitting());
        }
        when(updatedRPGPort.getMaxConcurrentTasks()).thenReturn(inputRPGPortDTO.getConcurrentlySchedulableTaskCount());
        when(updatedRPGPort.isUseCompression()).thenReturn(inputRPGPortDTO.getUseCompression());

        final BatchSettingsDTO batchSettings = inputRPGPortDTO.getBatchSettings();
        if (batchSettings != null) {
            when(updatedRPGPort.getBatchCount()).thenReturn(batchSettings.getCount());
            when(updatedRPGPort.getBatchSize()).thenReturn(batchSettings.getSize());
            when(updatedRPGPort.getBatchDuration()).thenReturn(batchSettings.getDuration());
        }

        when(joinPoint.proceed()).thenReturn(updatedRPGPort);

        // Capture added actions so that those can be asserted later.
        final AuditService auditService = mock(AuditService.class);
        final AtomicReference<Collection<Action>> addedActions = new AtomicReference<>();
        doAnswer(invocation -> {
            Collection<Action> actions = invocation.getArgumentAt(0, Collection.class);
            addedActions.set(actions);
            return null;
        }).when(auditService).addActions(any());

        auditor.setAuditService(auditService);

        auditor.auditUpdateProcessGroupInputPortConfiguration(joinPoint, remoteProcessGroupId, inputRPGPortDTO, remoteProcessGroupDAO);


        final Collection<Action> actions = addedActions.get();

        // Assert common action values.
        if (actions != null) {
            actions.forEach(action -> {
                assertEquals(remoteProcessGroupId, action.getSourceId());
                assertEquals("user-id", action.getUserIdentity());
                assertEquals(targetUrl, ((RemoteProcessGroupDetails)action.getComponentDetails()).getUri());
                assertNotNull(action.getTimestamp());
            });
        }

        return actions;
    }

    private RemoteGroupPort defaultRemoteGroupPort() {
        final RemoteGroupPort existingRPGPort = mock(RemoteGroupPort.class);
        when(existingRPGPort.isRunning()).thenReturn(false);
        when(existingRPGPort.getMaxConcurrentTasks()).thenReturn(1);
        when(existingRPGPort.isUseCompression()).thenReturn(false);
        return existingRPGPort;
    }

    private RemoteProcessGroupPortDTO defaultRemoteProcessGroupPortDTO() {
        final RemoteProcessGroupPortDTO inputRPGPortDTO = new RemoteProcessGroupPortDTO();
        inputRPGPortDTO.setConcurrentlySchedulableTaskCount(1);
        inputRPGPortDTO.setUseCompression(false);
        return inputRPGPortDTO;
    }

    @Test
    public void testEnablePort() throws Throwable {

        final RemoteGroupPort existingRPGPort = defaultRemoteGroupPort();
        when(existingRPGPort.getName()).thenReturn("input-port-1");
        when(existingRPGPort.isRunning()).thenReturn(false);

        final RemoteProcessGroupPortDTO inputRPGPortDTO = defaultRemoteProcessGroupPortDTO();
        inputRPGPortDTO.setTransmitting(true);

        final Collection<Action> actions = updateProcessGroupInputPortConfiguration(inputRPGPortDTO, existingRPGPort);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Transmission", "disabled", "enabled");

    }

    @Test
    public void testDisablePort() throws Throwable {

        final RemoteGroupPort existingRPGPort = defaultRemoteGroupPort();
        when(existingRPGPort.getName()).thenReturn("input-port-1");
        when(existingRPGPort.isRunning()).thenReturn(true);

        final RemoteProcessGroupPortDTO inputRPGPortDTO = defaultRemoteProcessGroupPortDTO();
        inputRPGPortDTO.setTransmitting(false);

        final Collection<Action> actions = updateProcessGroupInputPortConfiguration(inputRPGPortDTO, existingRPGPort);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Transmission", "enabled", "disabled");

    }

    @Test
    public void testConfigurePortConcurrency() throws Throwable {

        final RemoteGroupPort existingRPGPort = defaultRemoteGroupPort();
        when(existingRPGPort.getName()).thenReturn("input-port-1");

        final RemoteProcessGroupPortDTO inputRPGPortDTO = defaultRemoteProcessGroupPortDTO();
        inputRPGPortDTO.setConcurrentlySchedulableTaskCount(2);

        final Collection<Action> actions = updateProcessGroupInputPortConfiguration(inputRPGPortDTO, existingRPGPort);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Concurrent Tasks", "1", "2");

    }

    @Test
    public void testConfigurePortCompression() throws Throwable {

        final RemoteGroupPort existingRPGPort = defaultRemoteGroupPort();
        when(existingRPGPort.getName()).thenReturn("input-port-1");

        final RemoteProcessGroupPortDTO inputRPGPortDTO = defaultRemoteProcessGroupPortDTO();
        inputRPGPortDTO.setUseCompression(true);

        final Collection<Action> actions = updateProcessGroupInputPortConfiguration(inputRPGPortDTO, existingRPGPort);

        assertEquals(1, actions.size());
        final Action action = actions.iterator().next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Compressed", "false", "true");

    }

    @Test
    public void testConfigurePortBatchSettings() throws Throwable {

        final RemoteGroupPort existingRPGPort = defaultRemoteGroupPort();
        when(existingRPGPort.getName()).thenReturn("input-port-1");

        final RemoteProcessGroupPortDTO inputRPGPortDTO = defaultRemoteProcessGroupPortDTO();
        final BatchSettingsDTO batchSettingsDTO = new BatchSettingsDTO();
        batchSettingsDTO.setCount(1234);
        batchSettingsDTO.setSize("64KB");
        batchSettingsDTO.setDuration("10sec");
        inputRPGPortDTO.setBatchSettings(batchSettingsDTO);

        final Collection<Action> actions = updateProcessGroupInputPortConfiguration(inputRPGPortDTO, existingRPGPort);

        assertEquals(3, actions.size());
        final Iterator<Action> iterator = actions.iterator();
        Action action = iterator.next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Batch Count", "0", "1234");

        action = iterator.next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Batch Size", "", "64KB");

        action = iterator.next();
        assertEquals(Operation.Configure, action.getOperation());
        assertConfigureDetails(action.getActionDetails(), "input-port-1.Batch Duration", "", "10sec");
    }
}
