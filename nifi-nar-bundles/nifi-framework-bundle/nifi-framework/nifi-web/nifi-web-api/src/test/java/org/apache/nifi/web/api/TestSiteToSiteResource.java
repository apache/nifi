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
package org.apache.nifi.web.api;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSiteToSiteResource {

    @BeforeAll
    public static void setup() throws Exception {
        final URL resource = TestSiteToSiteResource.class.getResource("/site-to-site/nifi.properties");
        final String propertiesFile = resource.toURI().getPath();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile);
    }

    @Test
    public void testGetControllerForOlderVersion() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getSiteToSiteDetails();

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);
        final Response response = resource.getSiteToSiteDetails(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertNull(resultEntity.getController().getRemoteSiteHttpListeningPort(),"remoteSiteHttpListeningPort should be null since older version doesn't recognize this field" +
                " and throws JSON mapping exception.");
        assertEquals(Integer.valueOf(9990), controllerEntity.getController().getRemoteSiteListeningPort(),"Other fields should be retained.");
    }

    @Test
    public void testGetController() {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getSiteToSiteDetails();

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);
        final Response response = resource.getSiteToSiteDetails(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(Integer.valueOf(8080), resultEntity.getController().getRemoteSiteHttpListeningPort(),"remoteSiteHttpListeningPort should be retained");
        assertEquals(Integer.valueOf(9990), controllerEntity.getController().getRemoteSiteListeningPort(), "Other fields should be retained.");
    }

    private HttpServletRequest createCommonHttpServletRequest() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("1").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));
        return req;
    }

    @Test
    public void testPeers() {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
        final PeerDTO peer = resultEntity.getPeers().iterator().next();
        assertEquals(8080, peer.getPort());
    }

    @Test
    public void testPeersPortForwarding() {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.WEB_HTTP_PORT_FORWARDING, "80");
        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade, additionalProperties);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
        final PeerDTO peer = resultEntity.getPeers().iterator().next();
        assertEquals(80, peer.getPort());
    }

    @Test
    public void testPeersClustered() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final Map<String, String> clusterSettings = new HashMap<>();
        clusterSettings.put(NiFiProperties.CLUSTER_IS_NODE, "true");
        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade, clusterSettings);

        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final Map<String, NodeWorkload> hostportWorkloads = new HashMap<>();
        final Map<NodeIdentifier, NodeWorkload> workloads = new HashMap<>();
        IntStream.range(1, 4).forEach(i -> {
            final String hostname = "node" + i;
            final int siteToSiteHttpApiPort = 8110 + i;
            final NodeIdentifier nodeId = new NodeIdentifier(hostname, hostname, 8080 + i, hostname, 8090 + i, hostname, 8100 + i, siteToSiteHttpApiPort, false);
            final NodeWorkload workload = new NodeWorkload();
            workload.setReportedTimestamp(System.currentTimeMillis() - i);
            workload.setFlowFileBytes(1024L * i);
            workload.setFlowFileCount(10 * i);
            workload.setActiveThreadCount(i);
            workload.setSystemStartTime(System.currentTimeMillis() - (1000L * i));
            workloads.put(nodeId, workload);
            hostportWorkloads.put(hostname + ":" + siteToSiteHttpApiPort, workload);
        });
        when(clusterCoordinator.getClusterWorkload()).thenReturn(workloads);
        resource.setClusterCoordinator(clusterCoordinator);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(3, resultEntity.getPeers().size());
        resultEntity.getPeers().forEach(peerDTO -> {
            final NodeWorkload workload = hostportWorkloads.get(peerDTO.getHostname() + ":" + peerDTO.getPort());
            assertNotNull(workload);
            assertEquals(workload.getFlowFileCount(), peerDTO.getFlowFileCount());
        });

    }

    @Test
    public void testPeersVersionWasNotSpecified() {
        final HttpServletRequest req = mock(HttpServletRequest.class);

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();
        assertEquals(400, response.getStatus());
        assertEquals(ResponseCode.ABORT.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testPeersVersionNegotiationDowngrade() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("999").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
        assertEquals(1, response.getMetadata().getFirst(HttpHeaders.PROTOCOL_VERSION));
    }

    private SiteToSiteResource getSiteToSiteResource(final NiFiServiceFacade serviceFacade) {
        return getSiteToSiteResource(serviceFacade, null);
    }

    private SiteToSiteResource getSiteToSiteResource(final NiFiServiceFacade serviceFacade, final Map<String, String> additionalProperties) {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        final SiteToSiteResource resource = new SiteToSiteResource(properties) {
            @Override
            protected void authorizeSiteToSite() {
            }
        };
        resource.setProperties(properties);
        resource.setServiceFacade(serviceFacade);
        return resource;
    }
}