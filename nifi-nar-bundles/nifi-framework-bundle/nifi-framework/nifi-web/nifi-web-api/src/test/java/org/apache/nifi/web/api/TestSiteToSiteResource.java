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

import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TestSiteToSiteResource {

    @BeforeClass
    public static void setup() throws Exception {
        final URL resource = TestSiteToSiteResource.class.getResource("/site-to-site/nifi.properties");
        final String propertiesFile = resource.toURI().getPath();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile);
    }

    @Test
    public void testGetControllerForOlderVersion() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getController();

        final SiteToSiteResource resource = new SiteToSiteResource();
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);
        final Response response = resource.getSiteToSite(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertNull("remoteSiteHttpListeningPort should be null since older version doesn't recognize this field" +
                " and throws JSON mapping exception.", resultEntity.getController().getRemoteSiteHttpListeningPort());
        assertEquals("Other fields should be retained.", new Integer(9990), controllerEntity.getController().getRemoteSiteListeningPort());
    }

    @Test
    public void testGetController() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getController();

        final SiteToSiteResource resource = new SiteToSiteResource();
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);
        final Response response = resource.getSiteToSite(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals("remoteSiteHttpListeningPort should be retained", new Integer(8080), resultEntity.getController().getRemoteSiteHttpListeningPort());
        assertEquals("Other fields should be retained.", new Integer(9990), controllerEntity.getController().getRemoteSiteListeningPort());
    }

    private HttpServletRequest createCommonHttpServletRequest() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("1").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));
        return req;
    }

    @Test
    public void testPeers() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = new SiteToSiteResource();
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);

        final Response response = resource.getPeers(null, req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
    }


    @Test
    public void testPeersVersionWasNotSpecified() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = new SiteToSiteResource();
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);

        final Response response = resource.getPeers(null, req);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();
        assertEquals(400, response.getStatus());
        assertEquals(ResponseCode.ABORT.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testPeersVersionNegotiationDowngrade() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("999").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = new SiteToSiteResource();
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);

        final Response response = resource.getPeers(null, req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
        assertEquals(new Integer(1), response.getMetadata().getFirst(HttpHeaders.PROTOCOL_VERSION));
    }

    @Test
    public void testCreateTransactionPortNotFound() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);

        doThrow(new HandshakeException(ResponseCode.UNKNOWN_PORT, "Not found.")).when(serverProtocol).handshake(any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction(clientId, "input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(404, response.getStatus());
        assertEquals(ResponseCode.UNKNOWN_PORT.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCreateTransactionPortNotInValidState() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);

        doThrow(new HandshakeException(ResponseCode.PORT_NOT_IN_VALID_STATE, "Not in valid state.")).when(serverProtocol).handshake(any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction(clientId, "input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(503, response.getStatus());
        assertEquals(ResponseCode.PORT_NOT_IN_VALID_STATE.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCreateTransactionUnauthorized() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);

        doThrow(new HandshakeException(ResponseCode.UNAUTHORIZED, "Unauthorized.")).when(serverProtocol).handshake(any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction(clientId, "input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(401, response.getStatus());
        assertEquals(ResponseCode.UNAUTHORIZED.getCode(), resultEntity.getResponseCode());
    }

    private UriInfo mockUriInfo(final String locationUriStr) throws URISyntaxException {
        final UriInfo uriInfo = mock(UriInfo.class);
        final UriBuilder uriBuilder = mock(UriBuilder.class);

        final URI locationUri = new URI(locationUriStr);
        doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();
        doReturn(uriBuilder).when(uriBuilder).path(any(String.class));
        doReturn(locationUri).when(uriBuilder).build();
        return uriInfo;
    }

    @Test
    public void testCreateTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        mockHttpFlowFileServerProtocol(resource);

        final String locationUriStr = "http://localhost:8080/nifi-api/site-to-site/input-ports/port-id/transactions/transaction-id";

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final UriInfo uriInfo = mockUriInfo(locationUriStr);
        final InputStream inputStream = null;

       final Response response = resource.createPortTransaction(clientId, "input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(201, response.getStatus());
        assertEquals(ResponseCode.PROPERTIES_OK.getCode(), resultEntity.getResponseCode());
        assertEquals(locationUriStr, response.getMetadata().getFirst(HttpHeaders.LOCATION_HEADER_NAME).toString());
    }

    @Test
    public void testExtendTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        mockHttpFlowFileServerProtocol(resource);

        final String locationUriStr = "http://localhost:8080/nifi-api/site-to-site/input-ports/port-id/transactions/transaction-id";

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final HttpServletResponse res = null;
        final UriInfo uriInfo = mockUriInfo(locationUriStr);
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.extendPortTransactionTTL(clientId, "input-ports", "port-id", transactionId, req, res, context, uriInfo, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONTINUE_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testReceiveFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);

        final RootGroupPort port = mock(RootGroupPort.class);
        doReturn(port).when(serverProtocol).getPort();
        doAnswer(invocation -> {
            Peer peer = (Peer) invocation.getArguments()[0];
            ((HttpServerCommunicationsSession)peer.getCommunicationsSession()).setChecksum("server-checksum");
            return 7;
        }).when(port).receiveFlowFiles(any(Peer.class), any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.receiveFlowFiles(clientId, "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        final Object entity = response.getEntity();

        assertEquals(202, response.getStatus());
        assertEquals("server-checksum", entity);
    }

    @Test
    public void testReceiveZeroFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);

        final RootGroupPort port = mock(RootGroupPort.class);
        doReturn(port).when(serverProtocol).getPort();
        doAnswer(invocation -> 0).when(port).receiveFlowFiles(any(Peer.class), any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.receiveFlowFiles(clientId, "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testCommitInputPortTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        mockHttpFlowFileServerProtocol(resource);

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitInputPortTransaction(clientId, ResponseCode.CONFIRM_TRANSACTION.getCode(), "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONFIRM_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testTransferFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        mockHttpFlowFileServerProtocol(resource);

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final HttpServletResponse res = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.transferFlowFiles(clientId, "port-id", transactionId, req, res, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        final Object entity = response.getEntity();

        assertEquals(202, response.getStatus());
        assertTrue(entity instanceof StreamingOutput);
    }

    @Test
    public void testCommitOutputPortTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        mockHttpFlowFileServerProtocol(resource);

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitOutputPortTransaction(clientId, ResponseCode.CONFIRM_TRANSACTION.getCode(),
                "client-checksum", "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONFIRM_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCommitOutputPortTransactionBadChecksum() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = spySiteToSiteResource(serviceFacade);

        final HttpFlowFileServerProtocol serverProtocol = mockHttpFlowFileServerProtocol(resource);
        doThrow(new HandshakeException(ResponseCode.BAD_CHECKSUM, "Bad checksum.")).when(serverProtocol).commitTransferTransaction(any(), any());

        final ClientIdParameter clientId = new ClientIdParameter("client-id");
        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitOutputPortTransaction(clientId, ResponseCode.CONFIRM_TRANSACTION.getCode(),
                "client-checksum", "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(400, response.getStatus());
        assertEquals(ResponseCode.BAD_CHECKSUM.getCode(), resultEntity.getResponseCode());
    }

    private HttpFlowFileServerProtocol mockHttpFlowFileServerProtocol(SiteToSiteResource resource) {
        final HttpFlowFileServerProtocol serverProtocol = mock(HttpFlowFileServerProtocol.class);
        doReturn(serverProtocol).when(resource).getHttpFlowFileServerProtocol(any(VersionNegotiator.class));
        return serverProtocol;
    }

    private SiteToSiteResource spySiteToSiteResource(NiFiServiceFacade serviceFacade) {
        final SiteToSiteResource resource = spy(SiteToSiteResource.class);
        resource.setProperties(NiFiProperties.getInstance());
        resource.setServiceFacade(serviceFacade);
        return resource;
    }


}