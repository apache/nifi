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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.protocol.FlowFileRequest;
import org.apache.nifi.remote.protocol.ServerProtocol;

import java.util.Set;

/**
 * Represents an input or output port that can receive or transfer data via Site-to-Site protocol.
 */
public interface PublicPort extends Port {

    boolean isTransmitting();

    void setGroupAccessControl(Set<String> groups);

    Set<String> getGroupAccessControl();

    void setUserAccessControl(Set<String> users);

    Set<String> getUserAccessControl();

    /**
     * Verifies that the specified user is authorized to interact with this port
     * and returns a {@link PortAuthorizationResult} indicating why the user is
     * unauthorized if this assumption fails
     *
     * {@link #checkUserAuthorization(NiFiUser)} should be used if applicable
     * because NiFiUser has additional context such as chained user.
     *
     * @param dn dn of user
     * @return result
     */
    PortAuthorizationResult checkUserAuthorization(String dn);

    /**
     * Verifies that the specified user is authorized to interact with this port
     * and returns a {@link PortAuthorizationResult} indicating why the user is
     * unauthorized if this assumption fails
     *
     * @param user to authorize
     * @return result
     */
    PortAuthorizationResult checkUserAuthorization(NiFiUser user);

    /**
     * Receives data from the given stream.
     * This method blocks until receiving data completes.
     *
     * @param peer peer
     * @param serverProtocol protocol
     *
     * @return the number of FlowFiles received
     * @throws NotAuthorizedException nae
     * @throws BadRequestException bre
     * @throws RequestExpiredException ree
     */
    int receiveFlowFiles(Peer peer, ServerProtocol serverProtocol) throws NotAuthorizedException, BadRequestException, RequestExpiredException;

    /**
     * <p>Start receiving data from the given stream.
     *
     * <p>After submitting a data receiving request, this method returns immediately.
     * Then NiFi FlowController triggers corresponding InputPort to receive actual data using its thread pool.
     * This method can be used from SiteToSite server protocol implementation using non-blocking IO.
     *
     * <p>In order to get the response, use {@link FlowFileRequest#getResponseQueue()}.
     * A response object will be queued when the request has been processed.
     *
     * @param peer peer
     * @param serverProtocol protocol
     *
     * @return the submitted request
     */
    FlowFileRequest startReceivingFlowFiles(Peer peer, ServerProtocol serverProtocol);

    /**
     * Transfers data to the given stream
     *
     * @param peer peer
     * @param serverProtocol protocol
     *
     * @return the number of FlowFiles transferred
     * @throws NotAuthorizedException nae
     * @throws BadRequestException bre
     * @throws RequestExpiredException ree
     */
    int transferFlowFiles(Peer peer, ServerProtocol serverProtocol) throws NotAuthorizedException, BadRequestException, RequestExpiredException;

    TransferDirection getDirection();

    /**
     * <p>Start transferring data to the given stream.
     *
     * <p>After submitting a data transferring request, this method returns immediately.
     * Then NiFi FlowController triggers corresponding OutputPort to transfer actual data using its thread pool.
     * This method can be used from SiteToSite server protocol implementation using non-blocking IO.
     *
     * <p>In order to get the response, use {@link FlowFileRequest#getResponseQueue()}.
     * A response object will be queued when the request has been processed.
     *
     * @param peer peer
     * @param serverProtocol protocol
     *
     * @return the submitted request
     */
    FlowFileRequest startTransferringFlowFiles(Peer peer, ServerProtocol serverProtocol);

}
