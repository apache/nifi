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
package org.apache.nifi.remote;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Collections;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.RingBufferEventRepository;
import org.apache.nifi.controller.repository.StandardFlowFileEvent;
import org.apache.nifi.controller.repository.StandardRepositoryStatusReport;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.provenance.MockProvenanceEventRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RemoteProcessGroupStatusTest {

    private final UserService userService = mock(UserService.class);
    private final AuditService auditService = mock(AuditService.class);
    private volatile FlowController controller;

    @BeforeClass
    public static void beforeClass() {
        try {
            URL url = ClassLoader.getSystemClassLoader().getResource("nifi.properties");
            System.setProperty("nifi.properties.file.path", url.getFile());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to discover nifi.properties at the root of the classpath", e);
        }
    }

    @Before
    public void before() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }
        NiFiProperties properties = NiFiProperties.getInstance();
        properties.setProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceEventRepository.class.getName());
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "src/test/resources/state-management.xml");
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider");
        properties.setProperty(NiFiProperties.REMOTE_INPUT_HOST, "localhost");
        properties.setProperty(NiFiProperties.REMOTE_INPUT_PORT, String.valueOf(port));
        properties.setProperty("nifi.remote.input.secure", "false");

        RingBufferEventRepository repository = new RingBufferEventRepository(1);
        this.controller = FlowController.createStandaloneInstance(repository, NiFiProperties.getInstance(),
                this.userService, this.auditService, null);
    }

    @After
    public void after() {
        this.controller.shutdown(false);
    }

    /**
     * This test show that statistics are computed only if ports are
     * correctly connected with process groups.
     * @throws Exception exception
     */
    @Test
    public void testStatusCountersWhenPortsDisconnected() throws Exception {
        ProcessGroup receiverGroup = controller.createProcessGroup("SITE");
        setControllerRootGroup(this.controller, receiverGroup);

        RemoteProcessGroup remoteProcessGroup = controller.createRemoteProcessGroup("SENDER_REMOTE","http://foo:1234/nifi");
        receiverGroup.addRemoteProcessGroup(remoteProcessGroup);

        String inputPortId = "inputId";
        StandardRemoteProcessGroupPortDescriptor inputPortDescriptor = new StandardRemoteProcessGroupPortDescriptor();
        inputPortDescriptor.setId(inputPortId);
        inputPortDescriptor.setName("inputPort");
        remoteProcessGroup.setInputPorts(Collections.<RemoteProcessGroupPortDescriptor> singleton(inputPortDescriptor));
        RemoteGroupPort inputPort = remoteProcessGroup.getInputPort(inputPortId);
        inputPort.setProcessGroup(receiverGroup);

        String outputPortId = "outputId";
        StandardRemoteProcessGroupPortDescriptor outputPortDescriptor = new StandardRemoteProcessGroupPortDescriptor();
        outputPortDescriptor.setId(outputPortId);
        outputPortDescriptor.setName("outputPort");
        remoteProcessGroup.setOutputPorts(Collections.<RemoteProcessGroupPortDescriptor> singleton(outputPortDescriptor));
        RemoteGroupPort outputPort = remoteProcessGroup.getOutputPort(outputPortId);
        outputPort.setProcessGroup(receiverGroup);

        RepositoryStatusReport rp = new StandardRepositoryStatusReport();
        StandardFlowFileEvent inputEvent = new StandardFlowFileEvent(inputPortId);
        inputEvent.setBytesSent(5);
        rp.addReportEntry(inputEvent);

        StandardFlowFileEvent outputEvent = new StandardFlowFileEvent(outputPortId);
        outputEvent.setFlowFilesReceived(5);
        rp.addReportEntry(outputEvent);

        ProcessGroupStatus status = controller.getGroupStatus(receiverGroup, rp);
        assertEquals(0, status.getFlowFilesReceived());
        assertEquals(0, status.getBytesSent());
    }

    private void setControllerRootGroup(FlowController controller, ProcessGroup processGroup) {
        try {
            Method m = FlowController.class.getDeclaredMethod("setRootGroup", ProcessGroup.class);
            m.setAccessible(true);
            m.invoke(controller, processGroup);
            controller.initializeFlow();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set root group", e);
        }
    }

}