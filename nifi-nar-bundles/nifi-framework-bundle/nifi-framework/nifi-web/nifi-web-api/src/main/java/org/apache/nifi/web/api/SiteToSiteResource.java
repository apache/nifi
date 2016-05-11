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
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil.LOCATION_URI_INTENT_VALUE;

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

    private NiFiServiceFacade serviceFacade;
    private ControllerFacade controllerFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ResourceContext resourceContext;

    private final AtomicReference<ProcessGroup> rootGroup = new AtomicReference<>();

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
    public Response getController() {

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getController();

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
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
            return createNodeTypeErrorResponse(req.getPathInfo() + " is only accessible on NCM or Standalone NiFi instance.");
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

        PeersEntity entity = new PeersEntity();
        entity.setPeers(peers);

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("ports/{portId}/flow-files")
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
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        logger.debug("receiveFlowFiles request: portId={}", portId);

        if (properties.isClusterManager()) {
            return createNodeTypeErrorResponse(req.getPathInfo() + " is not available on a node in a NiFi cluster.");
        }

        RootGroupPort port = getRootGroupPort(portId, true);
        if(port == null){
            String message = "Input port was not found with id: " + portId;
            return Response.status(Response.Status.NOT_FOUND).type(message).build();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String txId = UUID.randomUUID().toString();
        Peer peer = initiatePeer(req, inputStream, out, txId);

        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);
            int numOfFlowFiles = port.receiveFlowFiles(peer, serverProtocol);
            logger.debug("finished receiving flow files, numOfFlowFiles={}", numOfFlowFiles);
            if (numOfFlowFiles < 1) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Client should send request when there is data to send. There was no flow file sent.").build();
            }
        } catch (IOException | NotAuthorizedException | BadRequestException | RequestExpiredException e) {
            // TODO: error handling.
            logger.error("Failed to process the request.", e);
            return Response.serverError().build();
        }

        String serverChecksum = ((HttpServerCommunicationsSession)peer.getCommunicationsSession()).getChecksum();
        return createLocationResult(false, portId, txId, serverChecksum);
    }

    private HttpFlowFileServerProtocol initiateServerProtocol(Peer peer, ServletContext context) throws IOException {
        // TODO: get rootGroup
        // Socket version impl is SocketRemoteSiteListener
        // serverProtocol.setRootProcessGroup(rootGroup.get());
        ConcurrentMap<String, FlowFileTransaction> txOnHold = (ConcurrentMap<String, FlowFileTransaction>)context.getAttribute(CONTEXT_ATTRIBUTE_TRANSACTION_ON_HOLD);
        HttpFlowFileServerProtocol serverProtocol = new HttpFlowFileServerProtocol(txOnHold);
        serverProtocol.setNodeInformant(clusterManager);
        serverProtocol.handshake(peer);
        return serverProtocol;
    }

    private Peer initiatePeer(HttpServletRequest req, InputStream inputStream, OutputStream outputStream, String txId) {
        String clientHostName = req.getRemoteHost();
        int clientPort = req.getRemotePort();

        PeerDescription peerDescription = new PeerDescription(clientHostName, clientPort, req.isSecure());

        CommunicationsSession commSession = new HttpServerCommunicationsSession(inputStream, outputStream, txId);

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
    @Path("ports/{portId}/tx/{transactionId}")
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
    public Response commitTxTransaction(
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

        logger.info("commitTxTransaction request: portId={} transactionId={}", portId, transactionId);

        if (properties.isClusterManager()) {
            return createNodeTypeErrorResponse(req.getPathInfo() + " is not available on a node in a NiFi cluster.");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Peer peer = initiatePeer(req, inputStream, out, transactionId);

        TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);
            int flowFileSent = serverProtocol.commitTransferTransaction(peer, checksum);
            // TODO: It'd be more natural to return TRANSACTION_FINISHED here.
            entity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
            entity.setFlowFileSent(flowFileSent);
        } catch (IOException e) {
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            logger.error("Failed to process the request", e);
            if(ResponseCode.BAD_CHECKSUM.equals(commsSession.getResponseCode())){
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setMessage(e.getMessage());

                Response.ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST).entity(entity);
                return clusterContext(noCache(builder)).build();
            }
            return Response.serverError().build();
        }

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    private Response createNodeTypeErrorResponse(String errMsg) {
        return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity(errMsg).build();
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("ports/{portId}/rx/{transactionId}")
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
    public Response commitRxTransaction(
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

        logger.info("commitRxTransaction request: portId={} transactionId={} responseCode={}", portId, transactionId, responseCode);

        if (properties.isClusterManager()) {
            return createNodeTypeErrorResponse(req.getPathInfo() + " is not available on a node in a NiFi cluster.");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Peer peer = initiatePeer(req, inputStream, out, transactionId);

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
                    throw e;
                }
            }
        } catch (IOException e) {
            logger.error("Failed to process the request.", e);
            return Response.serverError().build();
        }

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("ports/{portId}/flow-files")
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
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            InputStream inputStream) {

        logger.debug("transferFlowFiles request: portId={}", portId);

        if (properties.isClusterManager()) {
            return createNodeTypeErrorResponse(req.getPathInfo() + " is not available on a node in a NiFi cluster.");
        }

        RootGroupPort port = getRootGroupPort(portId, false);
        if(port == null){
            String message = "Output port was not found with id: " + portId;
            return Response.status(Response.Status.NOT_FOUND).type(message).build();
        }

        String txId = UUID.randomUUID().toString();

        StreamingOutput flowFileContent = new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                Peer peer = initiatePeer(req, inputStream, outputStream, txId);

                HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(peer, context);

                try {
                    int numOfFlowFiles = port.transferFlowFiles(peer, serverProtocol);
                    logger.debug("finished transferring flow files, numOfFlowFiles={}", numOfFlowFiles);
                    if(numOfFlowFiles < 1){
                        // There was no flow file to transfer. Throw this exception to stop responding with SEE OTHER.
                        throw new WebApplicationException(Response.Status.OK);
                    }
                } catch (NotAuthorizedException | BadRequestException | RequestExpiredException e) {
                    throw new IOException("Failed to process the request.", e);
                }
            }
        };

        return createLocationResult(true, portId, txId, flowFileContent);
    }

    private Response createLocationResult(boolean isTransfer, String portId, String txId, Object entity) {
        String locationUrl = "site-to-site/ports/" + portId + (isTransfer ? "/tx/" : "/rx/") + txId;
        URI location;
        try {
            location = new URI(locationUrl);
        } catch (URISyntaxException e){
            throw new RuntimeException("Failed to create a transaction uri", e);
        }
        return clusterContext(noCache(Response.seeOther(location)
                .header(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE))
                .entity(entity)).build();
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
}
