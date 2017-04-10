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

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface RemoteProcessGroup extends ComponentAuthorizable, Positionable {

    @Override
    String getIdentifier();

    String getTargetUri();

    String getTargetUris();

    ProcessGroup getProcessGroup();

    void setProcessGroup(ProcessGroup group);

    String getComments();

    void setComments(String comments);

    void shutdown();

    /**
     * @return the name of this RemoteProcessGroup. The value returned will
     * never be null. If unable to communicate with the remote instance, the URI
     * of that instance may be returned instead
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
     * @param timePeriod new period
     * @throws IllegalArgumentException iae
     */
    void setCommunicationsTimeout(String timePeriod) throws IllegalArgumentException;

    /**
     * @param timeUnit unit of time to report timeout
     * @return the communications timeout in terms of the given TimeUnit
     */
    int getCommunicationsTimeout(TimeUnit timeUnit);

    /**
     * @return the user-configured String representation of the communications
     * timeout
     */
    String getCommunicationsTimeout();

    /**
     * @return Indicates whether or not the RemoteProcessGroup is currently scheduled to
     * transmit data
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
     * @param port port to start
     */
    void startTransmitting(RemoteGroupPort port);

    /**
     * Immediately terminates communications between this instance and the
     * remote instance only for the port specified.
     *
     * @param port to stop
     */
    void stopTransmitting(RemoteGroupPort port);

    /**
     * @return Indicates whether or not communications with this RemoteProcessGroup will
     * be secure (2-way authentication)
     * @throws org.apache.nifi.controller.exception.CommunicationsException ce
     */
    boolean isSecure() throws CommunicationsException;

    /**
     * @return Indicates whether or not communications with this RemoteProcessGroup will
     * be secure (2-way authentication). Returns null if unknown.
     */
    Boolean getSecureFlag();

    /**
     * @return true if the target system has site to site enabled. Returns false
     * otherwise (they don't or they have not yet responded)
     */
    boolean isSiteToSiteEnabled();

    /**
     * @return a String indicating why we are not authorized to communicate with
     * the remote instance, or <code>null</code> if we are authorized
     */
    String getAuthorizationIssue();

    /**
     * Validates the current configuration, returning ValidationResults for any
     * invalid configuration parameter.
     *
     * @return Collection of validation result objects for any invalid findings
     *         only. If the collection is empty then the component is valid. Guaranteed
     *         non-null
     */
    Collection<ValidationResult> validate();

    /**
     * @return the {@link EventReporter} that can be used to report any notable
     * events
     */
    EventReporter getEventReporter();

    SiteToSiteTransportProtocol getTransportProtocol();

    void setTransportProtocol(SiteToSiteTransportProtocol transportProtocol);

    String getProxyHost();

    void setProxyHost(String proxyHost);

    Integer getProxyPort();

    void setProxyPort(Integer proxyPort);

    String getProxyUser();

    void setProxyUser(String proxyUser);

    String getProxyPassword();

    void setProxyPassword(String proxyPassword);

    void setNetworkInterface(String interfaceName);

    String getNetworkInterface();

    /**
     * Returns the InetAddress that the will this instance will bind to when communicating with a
     * remote NiFi instance, or <code>null</code> if no specific address has been specified
     */
    InetAddress getLocalAddress();

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
     * @param port to remove
     */
    void removeNonExistentPort(final RemoteGroupPort port);

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
}
