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
package org.apache.nifi.cluster.protocol;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;

/**
 * A node identifier denoting the coordinates of a flow controller that is
 * connected to a cluster. Nodes provide an external public API interface and an
 * internal private interface for communicating with the cluster.
 *
 * The external API interface and internal protocol each require an IP or
 * hostname as well as a port for communicating.
 *
 * This class overrides hashCode and equals and considers two instances to be
 * equal if they have the equal IDs.
 *
 * @Immutable
 * @Threadsafe
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeIdentifier {
    /**
     * the unique identifier for the node
     */
    private final String id;

    /**
     * the IP or hostname to use for sending requests to the node's external
     * interface
     */
    private final String apiAddress;

    /**
     * the port to use use for sending requests to the node's external interface,
     * this can be HTTP API port or HTTPS API port depending on whether //TODO: .
     */
    private final int apiPort;

    /**
     * the IP or hostname to use for sending requests to the node's internal
     * interface
     */
    private final String socketAddress;

    /**
     * the port to use use for sending requests to the node's internal interface
     */
    private final int socketPort;

    /**
     * the IP or hostname that external clients should use to communicate with this node via Site-to-Site
     */
    private final String siteToSiteAddress;

    /**
     * the port that external clients should use to communicate with this node via Site-to-Site RAW Socket protocol
     */
    private final Integer siteToSitePort;

    /**
     * the port that external clients should use to communicate with this node via Site-to-Site HTTP protocol,
     * this can be HTTP API port or HTTPS API port depending on whether siteToSiteSecure or not.
     */
    private final Integer siteToSiteHttpApiPort;

    /**
     * whether or not site-to-site communications with this node are secure
     */
    private final Boolean siteToSiteSecure;


    private final String nodeDn;

    public NodeIdentifier(final String id, final String apiAddress, final int apiPort, final String socketAddress, final int socketPort,
        final String siteToSiteAddress, final Integer siteToSitePort, final Integer siteToSiteHttpApiPort, final boolean siteToSiteSecure) {
        this(id, apiAddress, apiPort, socketAddress, socketPort, siteToSiteAddress, siteToSitePort, siteToSiteHttpApiPort, siteToSiteSecure, null);
    }

    public NodeIdentifier(final String id, final String apiAddress, final int apiPort, final String socketAddress, final int socketPort,
        final String siteToSiteAddress, final Integer siteToSitePort, final Integer siteToSiteHttpApiPort, final boolean siteToSiteSecure, final String dn) {

        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Node ID may not be empty or null.");
        } else if (StringUtils.isBlank(apiAddress)) {
            throw new IllegalArgumentException("Node API address may not be empty or null.");
        } else if (StringUtils.isBlank(socketAddress)) {
            throw new IllegalArgumentException("Node socket address may not be empty or null.");
        }

        validatePort(apiPort);
        validatePort(socketPort);
        if (siteToSitePort != null) {
            validatePort(siteToSitePort);
        }

        this.id = id;
        this.apiAddress = apiAddress;
        this.apiPort = apiPort;
        this.socketAddress = socketAddress;
        this.socketPort = socketPort;
        this.nodeDn = dn;
        this.siteToSiteAddress = siteToSiteAddress == null ? apiAddress : siteToSiteAddress;
        this.siteToSitePort = siteToSitePort;
        this.siteToSiteHttpApiPort = siteToSiteHttpApiPort;
        this.siteToSiteSecure = siteToSiteSecure;
    }

    /**
     * This constructor should not be used and exists solely for the use of JAXB
     */
    public NodeIdentifier() {
        this.id = null;
        this.apiAddress = null;
        this.apiPort = 0;
        this.socketAddress = null;
        this.socketPort = 0;
        this.nodeDn = null;
        this.siteToSiteAddress = null;
        this.siteToSitePort = null;
        this.siteToSiteHttpApiPort = null;
        this.siteToSiteSecure = false;
    }

    public String getId() {
        return id;
    }

    public String getDN() {
        return nodeDn;
    }

    public String getApiAddress() {
        return apiAddress;
    }

    public int getApiPort() {
        return apiPort;
    }

    public String getSocketAddress() {
        return socketAddress;
    }

    public int getSocketPort() {
        return socketPort;
    }

    private void validatePort(final int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be inclusively in the range [1, 65535].  Port given: " + port);
        }
    }

    public String getSiteToSiteAddress() {
        return siteToSiteAddress;
    }

    public Integer getSiteToSitePort() {
        return siteToSitePort;
    }

    public Integer getSiteToSiteHttpApiPort() {
        return siteToSiteHttpApiPort;
    }

    public boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }


    /**
     * Compares the id of two node identifiers for equality.
     *
     * @param obj a node identifier
     *
     * @return true if the id is equal; false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NodeIdentifier other = (NodeIdentifier) obj;
        if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
            return false;
        }
        return true;
    }

    /**
     * Compares API address/port and socket address/port for equality. The id is
     * not used for comparison.
     *
     * @param other a node identifier
     *
     * @return true if API address/port and socket address/port are equal; false
     * otherwise
     */
    public boolean logicallyEquals(final NodeIdentifier other) {
        if (other == null) {
            return false;
        }
        if ((this.apiAddress == null) ? (other.apiAddress != null) : !this.apiAddress.equals(other.apiAddress)) {
            return false;
        }
        if (this.apiPort != other.apiPort) {
            return false;
        }
        if ((this.socketAddress == null) ? (other.socketAddress != null) : !this.socketAddress.equals(other.socketAddress)) {
            return false;
        }
        if (this.socketPort != other.socketPort) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return apiAddress + ":" + apiPort;
    }

}
