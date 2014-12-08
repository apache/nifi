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
package org.apache.nifi.groups;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.CommunicationsSession;

public interface RemoteProcessGroup {

    String getIdentifier();

    URI getTargetUri();

    ProcessGroup getProcessGroup();

    void setProcessGroup(ProcessGroup group);

    void setPosition(Position position);

    Position getPosition();

    String getComments();

    void setComments(String comments);

    /**
     * Returns the name of this RemoteProcessGroup. The value returned will
     * never be null. If unable to communicate with the remote instance, the URI
     * of that instance may be returned instead
     *
     * @return
     */
    String getName();

    void setName(String name);

    void setInputPorts(Set<RemoteProcessGroupPortDescriptor> ports);

    void setOutputPorts(Set<RemoteProcessGroupPortDescriptor> ports);

    Set<RemoteGroupPort> getInputPorts();

    Set<RemoteGroupPort> getOutputPorts();

    RemoteGroupPort getInputPort(String id);

    RemoteGroupPort getOutputPort(String id);

    ProcessGroupCounts getCounts();

    void refreshFlowContents() throws CommunicationsException;

    Date getLastRefreshTime();

    void setYieldDuration(final String yieldDuration);

    String getYieldDuration();

    /**
     * Sets the timeout using the TimePeriod format (e.g., "30 secs", "1 min")
     *
     * @param timePeriod
     * @throws IllegalArgumentException
     */
    void setCommunicationsTimeout(String timePeriod) throws IllegalArgumentException;

    /**
     * Returns the communications timeout in terms of the given TimeUnit
     *
     * @param timeUnit
     * @return
     */
    int getCommunicationsTimeout(TimeUnit timeUnit);

    /**
     * Returns the user-configured String representation of the communications
     * timeout
     *
     * @return
     */
    String getCommunicationsTimeout();

    /**
     * @return the port that the remote instance is listening on for
     * site-to-site communication, or <code>null</code> if the remote instance
     * is not configured to allow site-to-site communications.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    Integer getListeningPort() throws IOException;

    /**
     * Indicates whether or not the RemoteProcessGroup is currently scheduled to
     * transmit data
     *
     * @return
     */
    boolean isTransmitting();

    /**
     * Initiates communications between this instance and the remote instance.
     */
    void startTransmitting();

    /**
     * Immediately terminates communications between this instance and the
     * remote instance.
     */
    void stopTransmitting();

    /**
     * Initiates communications between this instance and the remote instance
     * only for the port specified.
     *
     * @param port
     */
    void startTransmitting(RemoteGroupPort port);

    /**
     * Immediately terminates communications between this instance and the
     * remote instance only for the port specified.
     *
     * @param port
     */
    void stopTransmitting(RemoteGroupPort port);

    /**
     * Indicates whether or not communications with this RemoteProcessGroup will
     * be secure (2-way authentication)
     *
     * @return
     */
    boolean isSecure() throws CommunicationsException;

    /**
     * Indicates whether or not communications with this RemoteProcessGroup will
     * be secure (2-way authentication). Returns null if unknown.
     *
     * @return
     */
    Boolean getSecureFlag();

    /**
     * Returns true if the target system has site to site enabled. Returns false
     * otherwise (they don't or they have not yet responded).
     *
     * @return
     */
    boolean isSiteToSiteEnabled();

    /**
     * Returns a String indicating why we are not authorized to communicate with
     * the remote instance, or <code>null</code> if we are authorized
     *
     * @return
     */
    String getAuthorizationIssue();

    /**
     * Returns the {@link EventReporter} that can be used to report any notable
     * events
     *
     * @return
     */
    EventReporter getEventReporter();

    /**
     * Initiates a task in the remote process group to re-initialize, as a
     * result of clustering changes
     *
     * @param isClustered whether or not this instance is now clustered
     */
    void reinitialize(boolean isClustered);

    /**
     * Removes all non existent ports from this RemoteProcessGroup.
     */
    void removeAllNonExistentPorts();

    /**
     * Removes a port that no longer exists on the remote instance from this
     * RemoteProcessGroup
     *
     * @param port
     */
    void removeNonExistentPort(final RemoteGroupPort port);

    /**
     *
     * @return @throws IOException
     */
    CommunicationsSession establishSiteToSiteConnection() throws IOException;

    /**
     * Called whenever RemoteProcessGroup is removed from the flow, so that any
     * resources can be cleaned up appropriately.
     */
    void onRemove();

    void verifyCanDelete();

    void verifyCanDelete(boolean ignoreConnections);

    void verifyCanStartTransmitting();

    void verifyCanStopTransmitting();

    void verifyCanUpdate();

    /**
     * Returns a set of PeerStatus objects that describe the different peers
     * that we can communicate with for this RemoteProcessGroup.
     *
     * If the destination is a cluster, this set will contain PeerStatuses for
     * each of the nodes in the cluster.
     *
     * If the destination is a standalone instance, this set will contain just a
     * PeerStatus for the destination.
     *
     * Once the PeerStatuses have been obtained, they may be cached by this
     * RemoteProcessGroup for some amount of time.
     *
     * If unable to obtain the PeerStatuses or no peer status has yet been
     * obtained, will return null.
     *
     * @return
     */
    Set<PeerStatus> getPeerStatuses();
}
