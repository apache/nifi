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

import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.socket.HandshakeProperty;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.controller.ControllerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.ACCEPT_ENCODING;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;
import static org.apache.nifi.remote.protocol.socket.HandshakeProperty.BATCH_COUNT;
import static org.apache.nifi.remote.protocol.socket.HandshakeProperty.BATCH_DURATION;
import static org.apache.nifi.remote.protocol.socket.HandshakeProperty.BATCH_SIZE;
import static org.apache.nifi.remote.protocol.socket.HandshakeProperty.REQUEST_EXPIRATION_MILLIS;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Path("/site-to-site")
@Api(
        value = "/site-to-site",
        description = "Provide access to site to site with this NiFi"
)
public class SiteToSiteResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteResource.class);

    public static final String CHECK_SUM = "checksum";
    public static final String RESPONSE_CODE = "responseCode";
    public static final String CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD = "siteToSiteTransactionOnHold";


    private static final String PORT_TYPE_INPUT = "input-ports";
    private static final String PORT_TYPE_OUTPUT = "output-ports";

    private NiFiServiceFacade serviceFacade;
    private ControllerFacade controllerFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private final ResponseCreator responseCreator = new ResponseCreator();

    @Context
    private ResourceContext resourceContext;

    private final AtomicReference<ProcessGroup> rootGroup = new AtomicReference<>();

    /**
     * Returns the details of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_NIFI')")
    @ApiOperation(
            value = "Returns the details about this NiFi necessary to communicate via site to site",
            response = ControllerEntity.class,
            authorizations = @Authorization(value = "NiFi", type = "ROLE_NIFI")
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getController(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getController();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
        entity.setRevision(revision);
        entity.setController(controller);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }


    /**
     * Returns the details of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    // TODO: @PreAuthorize("hasRole('ROLE_NIFI')")
    @ApiOperation(
            value = "Returns the details about this NiFi necessary to communicate via site to site",
            response = PeersEntity.class,
            authorizations = @Authorization(value = "NiFi", type = "ROLE_NIFI")
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getPeers(
            @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
    )
    @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
    @Context HttpServletRequest req) {

        ArrayList<PeerDTO> peers;

        if (properties.isNode()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is only accessible on NCM or Standalone NiFi instance.");
        } else if (properties.isClusterManager()) {
            ClusterNodeInformation clusterNodeInfo = clusterManager.getNodeInformation();
            final Collection<NodeInformation> nodeInfos = clusterNodeInfo.getNodeInformation();
            peers = new ArrayList<>(nodeInfos.size());
            for(NodeInformation nodeInfo : nodeInfos){
                PeerDTO peer = new PeerDTO();
                // TODO: Need to set API host name instead.
                peer.setHostname(nodeInfo.getSiteToSiteHostname());
                // TODO: Determine which hostname:port to use based on isSiteToSiteSecure.
                // This port is configured in a context of NiFi Cluster Manager's use,
                // nodeInfo.getAPIPort() returns nifi.web.http.port or nifi.web.https.port
                // based on nifi.cluster.protocol.is.secure.
                peer.setPort(nodeInfo.getAPIPort());
                peer.setSecure(nodeInfo.isSiteToSiteSecure());
                peer.setFlowFileCount(nodeInfo.getTotalFlowFiles());
                peers.add(peer);
            }
        } else {
            // Standalone mode.
            // If this request is sent via HTTPS, subsequent requests should be sent via HTTPS.
            PeerDTO peer = new PeerDTO();
            peer.setHostname(req.getLocalName());
            peer.setPort(req.getLocalPort());
            peer.setSecure(req.isSecure());
            peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

            peers = new ArrayList<>(1);
            peers.add(peer);

        }

        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        PeersEntity entity = new PeersEntity();
        entity.setPeers(peers);

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{portType}/{portId}/transactions")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Create a transaction to this output port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response createPortTransaction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("portType") String portType,
            @PathParam("portId") String portId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        logger.debug("createPortTransaction request: clientId={}, portType={}, portId={}", clientId.getClientId(), portType, portId);

        if (properties.isClusterManager()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
        }

        if(!PORT_TYPE_INPUT.equals(portType) && !PORT_TYPE_OUTPUT.equals(portType)){
            responseCreator.wrongPortTypeResponse(clientId, portType, portId);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String transactionId = UUID.randomUUID().toString();
        Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        try {
            ConcurrentMap<String, FlowFileTransaction> transactionOnHold =
                    (ConcurrentMap<String, FlowFileTransaction>)context.getAttribute(CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD);

            // Execute handshake.
            initiateServerProtocol(peer, transactionOnHold);

            transactionOnHold.put(transactionId, new FlowFileTransaction());

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.PROPERTIES_OK.getCode());
            entity.setMessage("Handshake properties are valid, and port is running. A transaction is created:" + transactionId);

            return responseCreator.locationResponse(uriInfo, portType, portId, transactionId, entity);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files to input port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response receiveFlowFiles(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        logger.debug("receiveFlowFiles request: clientId={}, portId={}", clientId.getClientId(), portId);

        if (properties.isClusterManager()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
        }

        ConcurrentMap<String, FlowFileTransaction> transactionOnHold =
                (ConcurrentMap<String, FlowFileTransaction>)context.getAttribute(CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD);

        FlowFileTransaction transaction = transactionOnHold.remove(transactionId);
        if(transaction == null) {
            return responseCreator.transactionNotFoundResponse(clientId, portId, transactionId);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String receiveTransactionId = UUID.randomUUID().toString();
        Peer peer = constructPeer(req, inputStream, out, portId, receiveTransactionId);

        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);
            int numOfFlowFiles = serverProtocol.getPort().receiveFlowFiles(peer, serverProtocol);
            logger.debug("finished receiving flow files, numOfFlowFiles={}", numOfFlowFiles);
            if (numOfFlowFiles < 1) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Client should send request when there is data to send. There was no flow file sent.").build();
            }
        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (NotAuthorizedException e) {
            return responseCreator.unauthorizedResponse(e);

        } catch (BadRequestException | RequestExpiredException e) {
            return responseCreator.badRequestResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, e);
        }

        String serverChecksum = ((HttpServerCommunicationsSession)peer.getCommunicationsSession()).getChecksum();
        return responseCreator.locationResponse(uriInfo, PORT_TYPE_INPUT, portId, receiveTransactionId, serverChecksum);
    }

    private HttpFlowFileServerProtocol initiateServerProtocol(Peer peer, ServletContext context) throws IOException {
        ConcurrentMap<String, FlowFileTransaction> transactionOnHold =
                (ConcurrentMap<String, FlowFileTransaction>)context.getAttribute(CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD);

        return initiateServerProtocol(peer, transactionOnHold);
    }

    private HttpFlowFileServerProtocol initiateServerProtocol(Peer peer, ConcurrentMap<String, FlowFileTransaction> transactionOnHold) throws IOException {
        HttpFlowFileServerProtocol serverProtocol = new HttpFlowFileServerProtocol(transactionOnHold);
        HttpRemoteSiteListener.getInstance().setupServerProtocol(serverProtocol);
        serverProtocol.setNodeInformant(clusterManager);
        serverProtocol.handshake(peer);
        return serverProtocol;
    }

    private Peer constructPeer(HttpServletRequest req, InputStream inputStream, OutputStream outputStream, String portId, String transactionId) {
        String clientHostName = req.getRemoteHost();
        int clientPort = req.getRemotePort();

        PeerDescription peerDescription = new PeerDescription(clientHostName, clientPort, req.isSecure());

        HttpServerCommunicationsSession commSession = new HttpServerCommunicationsSession(inputStream, outputStream, transactionId);

        boolean gzip = false;
        Enumeration<String> acceptEncoding = req.getHeaders(ACCEPT_ENCODING);
        while (acceptEncoding.hasMoreElements()) {
            if ("gzip".equalsIgnoreCase(acceptEncoding.nextElement())) {
                gzip = true;
                break;
            }
        }

        final String requestExpiration = req.getHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION);
        final String batchCount = req.getHeader(HANDSHAKE_PROPERTY_BATCH_COUNT);
        final String batchSize = req.getHeader(HANDSHAKE_PROPERTY_BATCH_SIZE);
        final String batchDuration = req.getHeader(HANDSHAKE_PROPERTY_BATCH_DURATION);

        commSession.putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, portId);
        commSession.putHandshakeParam(HandshakeProperty.GZIP, String.valueOf(gzip));

        if (!isEmpty(requestExpiration)) commSession.putHandshakeParam(REQUEST_EXPIRATION_MILLIS, requestExpiration);
        if (!isEmpty(batchCount)) commSession.putHandshakeParam(BATCH_COUNT, batchCount);
        if (!isEmpty(batchSize)) commSession.putHandshakeParam(BATCH_SIZE, batchSize);
        if (!isEmpty(batchDuration)) commSession.putHandshakeParam(BATCH_DURATION, batchDuration);

        if(peerDescription.isSecure()){
            NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
            logger.debug("initiating peer, nifiUser={}", nifiUser);
            commSession.setUserDn(nifiUser.getIdentity());
        }

        // TODO: Followed how SocketRemoteSiteListener define peerUrl and clusterUrl, but it can be more meaningful values, especially for clusterUrl.
        String peerUrl = "nifi://" + clientHostName + ":" + clientPort;
        String clusterUrl = "nifi://localhost:" + req.getLocalPort();
        return new Peer(peerDescription, commSession, peerUrl, clusterUrl);
    }

    private RootGroupPort getRootGroupPort(String id, boolean input) {
        RootGroupPort port = null;
        for(RootGroupPort p : input ? controllerFacade.getInputPorts() : controllerFacade.getOutputPorts()){
            if(p.getIdentifier().equals(id)){
                port = p;
                break;
            }
        }

        return port;
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files to input port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response commitOutputPortTransaction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "A checksum calculated at client side using CRC32 to check flow file content integrity. It must be matched with the value calculated at server side.",
                    required = true
            )
            @QueryParam(CHECK_SUM) @DefaultValue(StringUtils.EMPTY) String checksum,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @ApiParam(
                    value = "The transaction id.",
                    required = true
            )
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        logger.debug("commitTxTransaction request: clientId={}, portId={}, transactionId={}", clientId, portId, transactionId);

        if (properties.isClusterManager()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);
            int flowFileSent = serverProtocol.commitTransferTransaction(peer, checksum);
            entity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
            entity.setFlowFileSent(flowFileSent);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            logger.error("Failed to process the request", e);
            if(ResponseCode.BAD_CHECKSUM.equals(commsSession.getResponseCode())){
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setMessage(e.getMessage());

                Response.ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST).entity(entity);
                return clusterContext(noCache(builder)).build();
            }

            return responseCreator.unexpectedErrorResponse(clientId, portId, transactionId, e);
        }

        return clusterContext(noCache(Response.ok(entity))).build();
    }


    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files to input port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response commitInputPortTransaction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The response code. Available values are BAD_CHECKSUM(19) or CONFIRM_TRANSACTION(12).",
                    required = true
            )
            @QueryParam(RESPONSE_CODE) Integer responseCode,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @ApiParam(
                    value = "The transaction id.",
                    required = true
            )
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        logger.debug("commitInputPortTransaction request: clientId={}, portId={}, transactionId={}, responseCode={}",
                clientId, portId, transactionId, responseCode);

        if (properties.isClusterManager()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            // Pass the response code sent from the client.
            String inputErrMessage = null;
            if(responseCode == null) {
                inputErrMessage = "responseCode is required.";
            } else if(ResponseCode.BAD_CHECKSUM.getCode() != responseCode
                    && ResponseCode.CONFIRM_TRANSACTION.getCode() != responseCode) {
                inputErrMessage = "responseCode " + responseCode + " is invalid. ";
            }

            if(inputErrMessage != null){
                entity.setMessage(inputErrMessage);
                entity.setResponseCode(ResponseCode.ABORT.getCode());
                return Response.status(Response.Status.BAD_REQUEST).entity(entity).build();
            }
            commsSession.setResponseCode(ResponseCode.fromCode(responseCode));

            try {
                int flowFileSent = serverProtocol.commitReceiveTransaction(peer);
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setFlowFileSent(flowFileSent);

            } catch (IOException e){
                if(ResponseCode.BAD_CHECKSUM.getCode() == responseCode && e.getMessage().contains("Received a BadChecksum response")){
                    // AbstractFlowFileServerProtocol throws IOException after it canceled transaction.
                    // This is a known behavior and if we return 500 with this exception,
                    // it's not clear if there is an issue at server side, or cancel operation has been accomplished.
                    // Above conditions can guarantee this is the latter case, we return 200 OK here.
                    entity.setResponseCode(ResponseCode.CANCEL_TRANSACTION.getCode());
                    return clusterContext(noCache(Response.ok(entity))).build();
                } else {
                    return responseCreator.unexpectedErrorResponse(clientId, portId, transactionId, e);
                }
            }

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, transactionId, e);
        }

        return clusterContext(noCache(Response.ok(entity))).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Receive flow files from output port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 204, message = "There is no flow file to return."),
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response transferFlowFiles(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        logger.debug("transferFlowFiles request: clientId={}, portId={}", clientId.getClientId(), portId);

        if (properties.isClusterManager()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
        }

        ConcurrentMap<String, FlowFileTransaction> transactionOnHold =
                (ConcurrentMap<String, FlowFileTransaction>)context.getAttribute(CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD);

        FlowFileTransaction transaction = transactionOnHold.remove(transactionId);
        if(transaction == null) {
            return responseCreator.transactionNotFoundResponse(clientId, portId, transactionId);
        }

        String transferTransactionId = UUID.randomUUID().toString();

        // Before opening the real output stream for HTTP response,
        // use this temporary output stream to buffer handshake result.
        final ByteArrayOutputStream tempBos = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, tempBos, portId, transferTransactionId);
        try {
            final HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);

            StreamingOutput flowFileContent = new StreamingOutput() {
                @Override
                public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                    HttpOutput output = (HttpOutput)peer.getCommunicationsSession().getOutput();
                    output.setOutputStream(outputStream);

                    try {
                        int numOfFlowFiles = serverProtocol.getPort().transferFlowFiles(peer, serverProtocol);
                        logger.debug("finished transferring flow files, numOfFlowFiles={}", numOfFlowFiles);
                        if(numOfFlowFiles < 1){
                            // There was no flow file to transfer. Throw this exception to stop responding with SEE OTHER.
                            throw new WebApplicationException(Response.Status.OK);
                        }
                    } catch (NotAuthorizedException | BadRequestException | RequestExpiredException e) {
                        // Handshake is done outside of write() method, so these exception wouldn't be thrown.
                        throw new IOException("Failed to process the request.", e);
                    }
                }
            };

            return responseCreator.locationResponse(uriInfo, PORT_TYPE_OUTPUT, portId, transferTransactionId, flowFileContent);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, e);
        }
    }


    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setControllerFacade(ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }


    private class ResponseCreator {

        private Response nodeTypeErrorResponse(String errMsg) {
            return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity(errMsg).build();
        }

        private Response wrongPortTypeResponse(ClientIdParameter clientId, String portType, String portId) {
            logger.debug("Transaction was not found. clientId={}, portType={}, portId={}", clientId.getClientId(), portType, portId);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Port was not found.");
            entity.setFlowFileSent(0);
            return Response.status(NOT_FOUND).entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        private Response transactionNotFoundResponse(ClientIdParameter clientId, String portId, String transactionId) {
            logger.debug("Transaction was not found. clientId={}, portId={}, transactionId={}", clientId.getClientId(), portId, transactionId);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Transaction was not found.");
            entity.setFlowFileSent(0);
            return Response.status(NOT_FOUND).entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        private Response unexpectedErrorResponse(ClientIdParameter clientId, String portId, Exception e) {
            logger.error("Unexpected exception occurred. clientId={}, portId={}", clientId.getClientId(), portId);
            logger.error("Exception detail:", e);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Server encountered an exception.");
            entity.setFlowFileSent(0);
            return Response.serverError().entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        private Response unexpectedErrorResponse(ClientIdParameter clientId, String portId, String transactionId, Exception e) {
            logger.error("Unexpected exception occurred. clientId={}, portId={}, transactionId={}", clientId.getClientId(), portId, transactionId);
            logger.error("Exception detail:", e);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Server encountered an exception.");
            entity.setFlowFileSent(0);
            return Response.serverError().entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        private Response unauthorizedResponse(NotAuthorizedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client request was not authorized.", e);
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.UNAUTHORIZED.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON_TYPE).entity(e.getMessage()).build();
        }

        private Response badRequestResponse(Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client sent a bad request.", e);
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
        }

        private Response handshakeExceptionResponse(HandshakeException e) {
            if(logger.isDebugEnabled()){
                logger.debug("Handshake failed", e);
            }
            ResponseCode handshakeRes = e.getResponseCode();
            Response.Status statusCd;
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(handshakeRes != null ? handshakeRes.getCode() : ResponseCode.ABORT.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            switch (handshakeRes) {
                case PORT_NOT_IN_VALID_STATE:
                case PORTS_DESTINATION_FULL:
                    return Response.status(Response.Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
                case UNAUTHORIZED:
                    statusCd = Response.Status.UNAUTHORIZED;
                    break;
                case UNKNOWN_PORT:
                    statusCd = NOT_FOUND;
                    break;
                default:
                    statusCd = Response.Status.BAD_REQUEST;
            }
            return Response.status(statusCd).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
        }

        private Response locationResponse(UriInfo uriInfo, String portType, String portId, String transactionId, Object entity) {
            String path = "/site-to-site/" + portType + "/" + portId + "/transactions/" + transactionId;
            URI location = uriInfo.getBaseUriBuilder().path(path).build();
            return noCache(Response.created(location)
                    .header(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE))
                    .entity(entity).build();
        }

    }
}
