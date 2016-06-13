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
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocolImpl;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
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
import javax.ws.rs.PUT;
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

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_COUNT;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_DURATION;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_SIZE;
import static org.apache.nifi.remote.protocol.HandshakeProperty.REQUEST_EXPIRATION_MILLIS;

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


    private static final String PORT_TYPE_INPUT = "input-ports";
    private static final String PORT_TYPE_OUTPUT = "output-ports";

    private NiFiServiceFacade serviceFacade;
    private final ResponseCreator responseCreator = new ResponseCreator();
    private final VersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);
    private final HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance();

    @Context
    private ResourceContext resourceContext;

    /**
     * Returns the details of this NiFi.
     *
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
            @Context HttpServletRequest req) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getController();

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
        entity.setController(controller);

        if (isEmpty(req.getHeader(HttpHeaders.PROTOCOL_VERSION))) {
            // This indicates the client uses older NiFi version,
            // which strictly read JSON properties and fail with unknown properties.
            // Convert result entity so that old version clients can understance.
            logger.debug("Converting result to provide backward compatibility...");
            controller.setRemoteSiteHttpListeningPort(null);
        }

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    private Response.ResponseBuilder setCommonHeaders(Response.ResponseBuilder builder, Integer transportProtocolVersion) {
        return builder.header(HttpHeaders.PROTOCOL_VERSION, transportProtocolVersion)
                .header(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL, transactionManager.getTransactionTtlSec());
    }

    private Integer negotiateTransportProtocolVersion(@Context HttpServletRequest req) throws BadRequestException {
        String protocolVersionStr = req.getHeader(HttpHeaders.PROTOCOL_VERSION);
        if (isEmpty(protocolVersionStr)) {
            throw new BadRequestException("Protocol version was not specified.");
        }

        final Integer requestedProtocolVersion;
        try {
            requestedProtocolVersion = Integer.valueOf(protocolVersionStr);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Specified protocol version was not in a valid number format: " + protocolVersionStr);
        }

        Integer protocolVersion;
        if (transportProtocolVersionNegotiator.isVersionSupported(requestedProtocolVersion)) {
            return requestedProtocolVersion;
        } else {
            protocolVersion = transportProtocolVersionNegotiator.getPreferredVersion(requestedProtocolVersion);
        }

        if (protocolVersion == null) {
            throw new BadRequestException("Specified protocol version is not supported: " + protocolVersionStr);
        }
        return protocolVersion;
    }


    /**
     * Returns the available Peers and its status of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A peersEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    // TODO: @PreAuthorize("hasRole('ROLE_NIFI')")
    @ApiOperation(
            value = "Returns the available Peers and its status of this NiFi",
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

        if (!properties.isSiteToSiteHttpEnabled()) {
            return responseCreator.httpSiteToSiteIsNotEnabledResponse();
        }

        final Integer transportProtocolVersion;
        try {
            transportProtocolVersion = negotiateTransportProtocolVersion(req);
        } catch (BadRequestException e) {
            return responseCreator.badRequestResponse(e);
        }

        ArrayList<PeerDTO> peers;

        if (properties.isNode()) {
            return responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is only accessible on NCM or Standalone NiFi instance.");
        // TODO: NCM no longer exists.
        /*
        } else if (properties.isClusterManager()) {
            ClusterNodeInformation clusterNodeInfo = clusterManager.getNodeInformation();
            final Collection<NodeInformation> nodeInfos = clusterNodeInfo.getNodeInformation();
            peers = new ArrayList<>(nodeInfos.size());
            for (NodeInformation nodeInfo : nodeInfos) {
                if (nodeInfo.getSiteToSiteHttpApiPort() == null) {
                    continue;
                }
                PeerDTO peer = new PeerDTO();
                peer.setHostname(nodeInfo.getSiteToSiteHostname());
                peer.setPort(nodeInfo.getSiteToSiteHttpApiPort());
                peer.setSecure(nodeInfo.isSiteToSiteSecure());
                peer.setFlowFileCount(nodeInfo.getTotalFlowFiles());
                peers.add(peer);
            }
        */
        } else {
            // Standalone mode.
            PeerDTO peer = new PeerDTO();
            // req.getLocalName returns private IP address, that can't be accessed from client in some environments.
            // So, use the value defined in nifi.properties instead when it is defined.
            String remoteInputHost = properties.getRemoteInputHost();
            peer.setHostname(isEmpty(remoteInputHost) ? req.getLocalName() : remoteInputHost);
            peer.setPort(properties.getRemoteInputHttpPort());
            peer.setSecure(properties.isSiteToSiteSecure());
            peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

            peers = new ArrayList<>(1);
            peers.add(peer);

        }

        PeersEntity entity = new PeersEntity();
        entity.setPeers(peers);

        return clusterContext(noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion))).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{portType}/{portId}/transactions")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Create a transaction to the specified output port or input port",
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
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
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

        final ValidateRequestResult validationResult = validateResult(req, clientId, portId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        if(!PORT_TYPE_INPUT.equals(portType) && !PORT_TYPE_OUTPUT.equals(portType)){
            return responseCreator.wrongPortTypeResponse(clientId, portType, portId);
        }

        logger.debug("createPortTransaction request: clientId={}, portType={}, portId={}", clientId.getClientId(), portType, portId);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final String transactionId = transactionManager.createTransaction();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;

        try {
            // Execute handshake.
            initiateServerProtocol(peer, transportProtocolVersion);

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.PROPERTIES_OK.getCode());
            entity.setMessage("Handshake properties are valid, and port is running. A transaction is created:" + transactionId);

            return responseCreator.locationResponse(uriInfo, portType, portId, transactionId, entity, transportProtocolVersion);

        } catch (HandshakeException e) {
            transactionManager.cancelTransaction(transactionId);
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            transactionManager.cancelTransaction(transactionId);
            return responseCreator.unexpectedErrorResponse(clientId, portId, e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("input-ports/{portId}/transactions/{transactionId}/flow-files")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files to the input port",
            response = String.class,
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
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
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
            InputStream inputStream) {

        final ValidateRequestResult validationResult = validateResult(req, clientId, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("receiveFlowFiles request: clientId={}, portId={}", clientId.getClientId(), portId);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;

        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, transportProtocolVersion);
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
        return responseCreator.acceptedResponse(serverChecksum, transportProtocolVersion);
    }

    private HttpFlowFileServerProtocol initiateServerProtocol(Peer peer, Integer transportProtocolVersion) throws IOException {
        // Switch transaction protocol version based on transport protocol version.
        TransportProtocolVersionNegotiator negotiatedTransportProtocolVersion = new TransportProtocolVersionNegotiator(transportProtocolVersion);
        VersionNegotiator versionNegotiator = new StandardVersionNegotiator(negotiatedTransportProtocolVersion.getTransactionProtocolVersion());
        HttpFlowFileServerProtocol serverProtocol = getHttpFlowFileServerProtocol(versionNegotiator);
        HttpRemoteSiteListener.getInstance().setupServerProtocol(serverProtocol);
        // TODO: How should I pass cluster information?
        // serverProtocol.setNodeInformant(clusterManager);
        serverProtocol.handshake(peer);
        return serverProtocol;
    }

    HttpFlowFileServerProtocol getHttpFlowFileServerProtocol(VersionNegotiator versionNegotiator) {
        return new HttpFlowFileServerProtocolImpl(versionNegotiator);
    }

    private Peer constructPeer(HttpServletRequest req, InputStream inputStream, OutputStream outputStream, String portId, String transactionId) {
        String clientHostName = req.getRemoteHost();
        int clientPort = req.getRemotePort();

        PeerDescription peerDescription = new PeerDescription(clientHostName, clientPort, req.isSecure());

        HttpServerCommunicationsSession commSession = new HttpServerCommunicationsSession(inputStream, outputStream, transactionId);

        boolean useCompression = false;
        final String useCompressionStr = req.getHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION);
        if (!isEmpty(useCompressionStr) && Boolean.valueOf(useCompressionStr)) {
            useCompression = true;
        }

        final String requestExpiration = req.getHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION);
        final String batchCount = req.getHeader(HANDSHAKE_PROPERTY_BATCH_COUNT);
        final String batchSize = req.getHeader(HANDSHAKE_PROPERTY_BATCH_SIZE);
        final String batchDuration = req.getHeader(HANDSHAKE_PROPERTY_BATCH_DURATION);

        commSession.putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, portId);
        commSession.putHandshakeParam(HandshakeProperty.GZIP, String.valueOf(useCompression));

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

    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Commit or cancel the specified transaction",
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
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            }
    )
    public Response commitOutputPortTransaction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The response code. Available values are CONFIRM_TRANSACTION(12) or CANCEL_TRANSACTION(15).",
                    required = true
            )
            @QueryParam(RESPONSE_CODE) Integer responseCode,
            @ApiParam(
                    value = "A checksum calculated at client side using CRC32 to check flow file content integrity. It must match with the value calculated at server side.",
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

        final ValidateRequestResult validationResult = validateResult(req, clientId, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }


        logger.debug("commitOutputPortTransaction request: clientId={}, portId={}, transactionId={}", clientId, portId, transactionId);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        final TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, transportProtocolVersion);

            String inputErrMessage = null;
            if (responseCode == null) {
                inputErrMessage = "responseCode is required.";
            } else if(ResponseCode.CONFIRM_TRANSACTION.getCode() != responseCode
                    && ResponseCode.CANCEL_TRANSACTION.getCode() != responseCode) {
                inputErrMessage = "responseCode " + responseCode + " is invalid. ";
            }

            if (inputErrMessage != null){
                entity.setMessage(inputErrMessage);
                entity.setResponseCode(ResponseCode.ABORT.getCode());
                return Response.status(Response.Status.BAD_REQUEST).entity(entity).build();
            }

            if (ResponseCode.CANCEL_TRANSACTION.getCode() == responseCode) {
                return cancelTransaction(transactionId, entity);
            }

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

        return clusterContext(noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion))).build();
    }


    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Commit or cancel the specified transaction",
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
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            }
    )
    public Response commitInputPortTransaction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The response code. Available values are BAD_CHECKSUM(19), CONFIRM_TRANSACTION(12) or CANCEL_TRANSACTION(15).",
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


        final ValidateRequestResult validationResult = validateResult(req, clientId, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("commitInputPortTransaction request: clientId={}, portId={}, transactionId={}, responseCode={}",
                clientId, portId, transactionId, responseCode);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        final TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, transportProtocolVersion);
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            // Pass the response code sent from the client.
            String inputErrMessage = null;
            if (responseCode == null) {
                inputErrMessage = "responseCode is required.";
            } else if(ResponseCode.BAD_CHECKSUM.getCode() != responseCode
                    && ResponseCode.CONFIRM_TRANSACTION.getCode() != responseCode
                    && ResponseCode.CANCEL_TRANSACTION.getCode() != responseCode) {
                inputErrMessage = "responseCode " + responseCode + " is invalid. ";
            }

            if (inputErrMessage != null){
                entity.setMessage(inputErrMessage);
                entity.setResponseCode(ResponseCode.ABORT.getCode());
                return Response.status(Response.Status.BAD_REQUEST).entity(entity).build();
            }

            if (ResponseCode.CANCEL_TRANSACTION.getCode() == responseCode) {
                return cancelTransaction(transactionId, entity);
            }

            commsSession.setResponseCode(ResponseCode.fromCode(responseCode));

            try {
                int flowFileSent = serverProtocol.commitReceiveTransaction(peer);
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setFlowFileSent(flowFileSent);

            } catch (IOException e){
                if (ResponseCode.BAD_CHECKSUM.getCode() == responseCode && e.getMessage().contains("Received a BadChecksum response")){
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

        return clusterContext(noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion))).build();
    }

    private Response cancelTransaction(String transactionId, TransactionResultEntity entity) {
        transactionManager.cancelTransaction(transactionId);
        entity.setMessage("Transaction has been canceled.");
        entity.setResponseCode(ResponseCode.CANCEL_TRANSACTION.getCode());
        return Response.ok(entity).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("output-ports/{portId}/transactions/{transactionId}/flow-files")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files from the output port",
            response = StreamingOutput.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "There is no flow file to return."),
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
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
            InputStream inputStream) {

        final ValidateRequestResult validationResult = validateResult(req, clientId, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("transferFlowFiles request: clientId={}, portId={}", clientId.getClientId(), portId);

        // Before opening the real output stream for HTTP response,
        // use this temporary output stream to buffer handshake result.
        final ByteArrayOutputStream tempBos = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, tempBos, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        try {
            final HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, transportProtocolVersion);

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

            return responseCreator.acceptedResponse(flowFileContent, transportProtocolVersion);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, e);
        }
    }

    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Extend transaction TTL",
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
    public Response extendInputPortTransactionTTL(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        return extendPortTransactionTTL(clientId, PORT_TYPE_INPUT, portId, transactionId, req, res, context, uriInfo, inputStream);

    }

    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Extend transaction TTL",
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
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful."),
                    @ApiResponse(code = 503, message = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            }
    )
    public Response extendOutputPortTransactionTTL(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        return extendPortTransactionTTL(clientId, PORT_TYPE_OUTPUT, portId, transactionId, req, res, context, uriInfo, inputStream);

    }

    public Response extendPortTransactionTTL(
            ClientIdParameter clientId,
            String portType,
            String portId,
            String transactionId,
            HttpServletRequest req,
            HttpServletResponse res,
            ServletContext context,
            UriInfo uriInfo,
            InputStream inputStream) {

        final ValidateRequestResult validationResult = validateResult(req, clientId, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        if(!PORT_TYPE_INPUT.equals(portType) && !PORT_TYPE_OUTPUT.equals(portType)){
            return responseCreator.wrongPortTypeResponse(clientId, portType, portId);
        }

        logger.debug("extendOutputPortTransactionTTL request: clientId={}, portType={}, portId={}, transactionId={}",
                clientId.getClientId(), portType, portId, transactionId);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        try {
            // Do handshake
            initiateServerProtocol(peer, transportProtocolVersion);
            transactionManager.extendsTransaction(transactionId);

            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.CONTINUE_TRANSACTION.getCode());
            entity.setMessage("Extended TTL.");
            return clusterContext(noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion))).build();

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(clientId, portId, transactionId, e);
        }

    }

    private class ValidateRequestResult {
        private Integer transportProtocolVersion;
        private Response errResponse;
    }

    private ValidateRequestResult validateResult(HttpServletRequest req, ClientIdParameter clientId, String portId) {
        return validateResult(req, clientId, portId, null);
    }

    private ValidateRequestResult validateResult(HttpServletRequest req, ClientIdParameter clientId, String portId, String transactionId) {
        ValidateRequestResult result = new ValidateRequestResult();
        if(!properties.isSiteToSiteHttpEnabled()) {
            result.errResponse = responseCreator.httpSiteToSiteIsNotEnabledResponse();
            return result;
        }

        // TODO: NCM no longer exists.
        /*
        if (properties.isClusterManager()) {
            result.errResponse = responseCreator.nodeTypeErrorResponse(req.getPathInfo() + " is not available on a NiFi Cluster Manager.");
            return result;
        }
        */


        try {
            result.transportProtocolVersion = negotiateTransportProtocolVersion(req);
        } catch (BadRequestException e) {
            result.errResponse = responseCreator.badRequestResponse(e);
            return result;
        }

        if(!isEmpty(transactionId) && !transactionManager.isTransactionActive(transactionId)) {
            result.errResponse = responseCreator.transactionNotFoundResponse(clientId, portId, transactionId);
            return  result;
        }

        return result;
    }


    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }


    private class ResponseCreator {

        private Response nodeTypeErrorResponse(String errMsg) {
            return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity(errMsg).build();
        }

        private Response httpSiteToSiteIsNotEnabledResponse() {
            return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity("HTTP(S) Site-to-Site is not enabled on this host.").build();
        }

        private Response wrongPortTypeResponse(ClientIdParameter clientId, String portType, String portId) {
            logger.debug("Port type was wrong. clientId={}, portType={}, portId={}", clientId.getClientId(), portType, portId);
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
                logger.debug("Client request was not authorized. {}", e.getMessage());
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.UNAUTHORIZED.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON_TYPE).entity(e.getMessage()).build();
        }

        private Response badRequestResponse(Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client sent a bad request. {}", e.getMessage());
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
        }

        private Response handshakeExceptionResponse(HandshakeException e) {
            if(logger.isDebugEnabled()){
                logger.debug("Handshake failed, {}", e.getMessage());
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

        private Response acceptedResponse(Object entity, Integer protocolVersion) {
            return noCache(setCommonHeaders(Response.status(Response.Status.ACCEPTED), protocolVersion))
                    .entity(entity).build();
        }

        private Response locationResponse(UriInfo uriInfo, String portType, String portId, String transactionId, Object entity, Integer protocolVersion) {
            String path = "/site-to-site/" + portType + "/" + portId + "/transactions/" + transactionId;
            URI location = uriInfo.getBaseUriBuilder().path(path).build();
            return noCache(setCommonHeaders(Response.created(location), protocolVersion)
                    .header(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE))
                    .entity(entity).build();
        }

    }
}
