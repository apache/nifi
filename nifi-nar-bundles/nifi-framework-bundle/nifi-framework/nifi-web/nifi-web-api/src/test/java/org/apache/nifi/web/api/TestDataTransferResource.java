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

import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.resource.ResourceType;
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
import org.apache.nifi.web.api.entity.TransactionResultEntity;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestDataTransferResource {

    @BeforeClass
    public static void setup() throws Exception {
        final URL resource = TestDataTransferResource.class.getResource("/site-to-site/nifi.properties");
        final String propertiesFile = resource.toURI().getPath();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile);
    }

    private HttpServletRequest createCommonHttpServletRequest() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("1").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));
        doReturn(new StringBuffer("http://nifi.example.com:8080")
                .append("/nifi-api/data-transfer/output-ports/port-id/transactions/tx-id/flow-files"))
                .when(req).getRequestURL();
        return req;
    }

    @Test
    public void testCreateTransactionPortNotFound() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        doThrow(new HandshakeException(ResponseCode.UNKNOWN_PORT, "Not found.")).when(serverProtocol).handshake(any());

        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction("input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(404, response.getStatus());
        assertEquals(ResponseCode.UNKNOWN_PORT.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCreateTransactionPortNotInValidState() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        doThrow(new HandshakeException(ResponseCode.PORT_NOT_IN_VALID_STATE, "Not in valid state.")).when(serverProtocol).handshake(any());

        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction("input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(503, response.getStatus());
        assertEquals(ResponseCode.PORT_NOT_IN_VALID_STATE.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCreateTransactionUnauthorized() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        doThrow(new HandshakeException(ResponseCode.UNAUTHORIZED, "Unauthorized.")).when(serverProtocol).handshake(any());

        final ServletContext context = null;
        final UriInfo uriInfo = null;
        final InputStream inputStream = null;

        final Response response = resource.createPortTransaction("input-ports", "port-id", req, context, uriInfo, inputStream);

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

        final DataTransferResource resource = getDataTransferResource();

        final String locationUriStr = "http://localhost:8080/nifi-api/data-transfer/input-ports/port-id/transactions/transaction-id";

        final ServletContext context = null;
        final UriInfo uriInfo = mockUriInfo(locationUriStr);
        final InputStream inputStream = null;

       final Response response = resource.createPortTransaction("input-ports", "port-id", req, context, uriInfo, inputStream);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(201, response.getStatus());
        assertEquals(ResponseCode.PROPERTIES_OK.getCode(), resultEntity.getResponseCode());
        assertEquals(locationUriStr, response.getMetadata().getFirst(HttpHeaders.LOCATION_HEADER_NAME).toString());
    }

    @Test
    public void testExtendTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();

        final String locationUriStr = "http://localhost:8080/nifi-api/data-transfer/input-ports/port-id/transactions/transaction-id";

        final ServletContext context = null;
        final HttpServletResponse res = null;
        final UriInfo uriInfo = mockUriInfo(locationUriStr);
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.extendPortTransactionTTL("input-ports", "port-id", transactionId, req, res, context, uriInfo, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONTINUE_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testReceiveFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        final RootGroupPort port = mock(RootGroupPort.class);
        doReturn(port).when(serverProtocol).getPort();
        doAnswer(invocation -> {
            Peer peer = (Peer) invocation.getArguments()[0];
            ((HttpServerCommunicationsSession)peer.getCommunicationsSession()).setChecksum("server-checksum");
            return 7;
        }).when(port).receiveFlowFiles(any(Peer.class), any());

        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.receiveFlowFiles("port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        final Object entity = response.getEntity();

        assertEquals(202, response.getStatus());
        assertEquals("server-checksum", entity);
    }

    @Test
    public void testReceiveZeroFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        final RootGroupPort port = mock(RootGroupPort.class);
        doReturn(port).when(serverProtocol).getPort();
        doAnswer(invocation -> 0).when(port).receiveFlowFiles(any(Peer.class), any());

        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.receiveFlowFiles("port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testCommitInputPortTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();

        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitInputPortTransaction(ResponseCode.CONFIRM_TRANSACTION.getCode(), "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONFIRM_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testTransferFlowFiles() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();

        final ServletContext context = null;
        final HttpServletResponse res = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.transferFlowFiles("port-id", transactionId, req, res, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        final Object entity = response.getEntity();

        assertEquals(202, response.getStatus());
        assertTrue(entity instanceof StreamingOutput);
    }

    @Test
    public void testCommitOutputPortTransaction() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();

        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitOutputPortTransaction(ResponseCode.CONFIRM_TRANSACTION.getCode(),
                "client-checksum", "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(ResponseCode.CONFIRM_TRANSACTION.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testCommitOutputPortTransactionBadChecksum() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final DataTransferResource resource = getDataTransferResource();
        final HttpFlowFileServerProtocol serverProtocol = resource.getHttpFlowFileServerProtocol(null);

        doThrow(new HandshakeException(ResponseCode.BAD_CHECKSUM, "Bad checksum.")).when(serverProtocol).commitTransferTransaction(any(), any());

        final ServletContext context = null;
        final InputStream inputStream = null;

        final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String transactionId = transactionManager.createTransaction();

        final Response response = resource.commitOutputPortTransaction(ResponseCode.CONFIRM_TRANSACTION.getCode(),
                "client-checksum", "port-id", transactionId, req, context, inputStream);

        transactionManager.cancelTransaction(transactionId);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();

        assertEquals(400, response.getStatus());
        assertEquals(ResponseCode.BAD_CHECKSUM.getCode(), resultEntity.getResponseCode());
    }

    private DataTransferResource getDataTransferResource() {
        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final HttpFlowFileServerProtocol serverProtocol = mock(HttpFlowFileServerProtocol.class);
        final DataTransferResource resource = new DataTransferResource(NiFiProperties.createBasicNiFiProperties(null, null)) {
            @Override
            protected void authorizeDataTransfer(AuthorizableLookup lookup, ResourceType resourceType, String identifier) {
            }

            @Override
            HttpFlowFileServerProtocol getHttpFlowFileServerProtocol(VersionNegotiator versionNegotiator) {
                return serverProtocol;
            }
        };
        resource.setProperties(NiFiProperties.createBasicNiFiProperties(null, null));
        resource.setServiceFacade(serviceFacade);
        return resource;
    }
}