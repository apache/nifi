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
package org.apache.nifi.cluster.flow.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.cluster.flow.DataFlowDao;
import org.apache.nifi.cluster.flow.PersistedFlowState;
import org.apache.nifi.cluster.protocol.ClusterManagerProtocolSender;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterManagerProtocolSenderImpl;
import org.apache.nifi.cluster.protocol.impl.SocketProtocolListener;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 */
public class DataFlowManagementServiceImplTest {

    private DataFlowManagementServiceImpl service;
    private File restoreLocation;
    private File primaryLocation;
    private DataFlowDao dao;
    private int apiDummyPort;
    private int socketPort;
    private SocketConfiguration socketConfig;
    private ClusterManagerProtocolSender sender;
    private ServerSocketConfiguration serverSocketConfig;
    private SocketProtocolListener listener;

    @Before
    public void setup() throws IOException {

        primaryLocation = new File(System.getProperty("java.io.tmpdir") + "/primary" + this.getClass().getSimpleName());
        restoreLocation = new File(System.getProperty("java.io.tmpdir") + "/restore" + this.getClass().getSimpleName());

        FileUtils.deleteDirectory(primaryLocation);
        FileUtils.deleteDirectory(restoreLocation);

        ProtocolContext protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);

        socketConfig = new SocketConfiguration();
        socketConfig.setSocketTimeout(1000);
        serverSocketConfig = new ServerSocketConfiguration();

        dao = new DataFlowDaoImpl(primaryLocation, restoreLocation, false);

        sender = new ClusterManagerProtocolSenderImpl(socketConfig, protocolContext);

        service = new DataFlowManagementServiceImpl(dao, sender);
        service.start();

        listener = new SocketProtocolListener(1, 0, serverSocketConfig, protocolContext);
        listener.start();

        apiDummyPort = 7777;
        socketPort = listener.getPort();
    }

    @After
    public void teardown() throws IOException {

        if (service != null && service.isRunning()) {
            service.stop();
        }

        if (listener != null && listener.isRunning()) {
            try {
                listener.stop();
            } catch (final Exception ex) {
                ex.printStackTrace(System.out);
            }
        }
        FileUtils.deleteDirectory(primaryLocation);
        FileUtils.deleteDirectory(restoreLocation);

    }

    @Test
    public void testLoadFlowWithNonExistentFlow() throws ParserConfigurationException, SAXException, IOException {
        verifyFlow();
    }

    @Test
    public void testLoadFlowWithNonExistentFlowWhenServiceStopped() throws IOException, SAXException, ParserConfigurationException {
        service.stop();
        verifyFlow();
    }

    private void verifyFlow() throws ParserConfigurationException, SAXException, IOException {
        final byte[] flowBytes = service.loadDataFlow().getDataFlow().getFlow();
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);

        final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        final Document doc = docBuilder.parse(new ByteArrayInputStream(flowBytes));
        final Element controller = (Element) doc.getElementsByTagName("flowController").item(0);
        final Element rootGroup = (Element) controller.getElementsByTagName("rootGroup").item(0);
        final String rootGroupName = rootGroup.getElementsByTagName("name").item(0).getTextContent();
        assertEquals("NiFi Flow", rootGroupName);
    }

    @Test
    public void testLoadFlowSingleNode() throws Exception {
        String flowStr = "<rootGroup />";
        byte[] flowBytes = flowStr.getBytes();
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));

        NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);
        service.setNodeIds(new HashSet<>(Arrays.asList(nodeId)));
        service.setPersistedFlowState(PersistedFlowState.STALE);

        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.CURRENT);

        assertEquals(PersistedFlowState.CURRENT, service.getPersistedFlowState());
        assertArrayEquals(flowBytes, service.loadDataFlow().getDataFlow().getFlow());

    }

    @Test
    public void testLoadFlowWithSameNodeIds() throws Exception {

        String flowStr = "<rootGroup />";
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));

        NodeIdentifier nodeId1 = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);
        NodeIdentifier nodeId2 = new NodeIdentifier("2", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);
        service.setNodeIds(new HashSet<>(Arrays.asList(nodeId1, nodeId2)));
        service.setPersistedFlowState(PersistedFlowState.STALE);

        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.CURRENT);

        // verify that flow is current
        assertEquals(PersistedFlowState.CURRENT, service.getPersistedFlowState());

        // add same ids in different order
        service.setNodeIds(new HashSet<>(Arrays.asList(nodeId2, nodeId1)));

        // verify flow is still current
        assertEquals(PersistedFlowState.CURRENT, service.getPersistedFlowState());

    }

    @Test
    public void testLoadFlowWithABadNode() throws Exception {

        String flowStr = "<rootGroup />";
        byte[] flowBytes = flowStr.getBytes();
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));

        NodeIdentifier nodeId1 = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort + 1, "localhost", 1234, false);
        NodeIdentifier nodeId2 = new NodeIdentifier("2", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);
        service.setNodeIds(new HashSet<>(Arrays.asList(nodeId1, nodeId2)));
        service.setPersistedFlowState(PersistedFlowState.STALE);

        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.CURRENT);

        assertEquals(PersistedFlowState.CURRENT, service.getPersistedFlowState());
        assertArrayEquals(flowBytes, service.loadDataFlow().getDataFlow().getFlow());

    }

    @Test
    public void testLoadFlowWithConstantNodeIdChanging() throws Exception {
        String flowStr = "<rootGroup />";
        byte[] flowBytes = flowStr.getBytes();
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));

        NodeIdentifier nodeId1 = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort + 1, "localhost", 1234, false);
        NodeIdentifier nodeId2 = new NodeIdentifier("2", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);

        for (int i = 0; i < 1000; i++) {
            service.setNodeIds(new HashSet<>(Arrays.asList(nodeId1, nodeId2)));
            service.setPersistedFlowState(PersistedFlowState.STALE);
            assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());
        }

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.CURRENT);

        assertEquals(PersistedFlowState.CURRENT, service.getPersistedFlowState());
        assertArrayEquals(flowBytes, service.loadDataFlow().getDataFlow().getFlow());
    }

    @Test
    public void testLoadFlowWithConstantNodeIdChangingWithRetrievalDelay() throws Exception {

        String flowStr = "<rootGroup />";
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));

        NodeIdentifier nodeId1 = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort + 1, "localhost", 1234, false);
        NodeIdentifier nodeId2 = new NodeIdentifier("2", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);

        service.setRetrievalDelay("5 sec");
        for (int i = 0; i < 1000; i++) {
            service.setNodeIds(new HashSet<>(Arrays.asList(nodeId1, nodeId2)));
            service.setPersistedFlowState(PersistedFlowState.STALE);
            assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());
        }

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.STALE);

        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

    }

    @Test
    public void testStopRequestedWhileRetrieving() throws Exception {

        String flowStr = "<rootGroup />";
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));
        Set<NodeIdentifier> nodeIds = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            nodeIds.add(new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort + 1, "localhost", 1234, false));
        }
        nodeIds.add(new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false));

        long lastRetrievalTime = service.getLastRetrievalTime();

        service.setNodeIds(nodeIds);
        service.setPersistedFlowState(PersistedFlowState.STALE);
        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.STALE);

        service.stop();

        service.setPersistedFlowState(PersistedFlowState.STALE);
        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        assertEquals(lastRetrievalTime, service.getLastRetrievalTime());

    }

    @Test
    public void testLoadFlowUnknownState() throws Exception {

        String flowStr = "<rootGroup />";
        byte[] flowBytes = flowStr.getBytes();
        listener.addHandler(new FlowRequestProtocolHandler(new StandardDataFlow(flowStr.getBytes("UTF-8"), new byte[0])));
        NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", apiDummyPort, "localhost", socketPort, "localhost", 1234, false);

        service.setNodeIds(new HashSet<>(Arrays.asList(nodeId)));
        service.setPersistedFlowState(PersistedFlowState.STALE);
        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        service.setPersistedFlowState(PersistedFlowState.UNKNOWN);

        assertEquals(PersistedFlowState.UNKNOWN, service.getPersistedFlowState());

        service.setPersistedFlowState(PersistedFlowState.STALE);
        assertEquals(PersistedFlowState.STALE, service.getPersistedFlowState());

        // sleep long enough for the flow retriever to run
        waitForState(PersistedFlowState.CURRENT);

        assertArrayEquals(flowBytes, service.loadDataFlow().getDataFlow().getFlow());

    }

    private class FlowRequestProtocolHandler implements ProtocolHandler {

        private StandardDataFlow dataFlow;

        public FlowRequestProtocolHandler(final StandardDataFlow dataFlow) {
            this.dataFlow = dataFlow;
        }

        @Override
        public boolean canHandle(ProtocolMessage msg) {
            return true;
        }

        @Override
        public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
            FlowResponseMessage response = new FlowResponseMessage();
            response.setDataFlow(dataFlow);
            return response;
        }

    }

    private void waitForState(PersistedFlowState state) throws InterruptedException {
        for (int i = 0; i < 30; i++) {
            if (service.getPersistedFlowState() == state) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }
    }
}
