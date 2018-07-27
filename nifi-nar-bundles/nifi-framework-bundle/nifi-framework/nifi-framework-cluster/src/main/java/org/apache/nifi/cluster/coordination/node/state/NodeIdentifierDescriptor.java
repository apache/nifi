package org.apache.nifi.cluster.coordination.node.state;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public class NodeIdentifierDescriptor {
    private String id;
    private String apiAddress;
    private int apiPort;
    private String socketAddress;
    private int socketPort;
    private String loadBalanceAddress;
    private int loadBalancePort;
    private String siteToSiteAddress;
    private Integer siteToSitePort;
    private Integer siteToSiteHttpApiPort;
    private Boolean siteToSiteSecure;
    private String nodeDn;
    private boolean localNodeIdentifier;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getApiAddress() {
        return apiAddress;
    }

    public void setApiAddress(final String apiAddress) {
        this.apiAddress = apiAddress;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(final int apiPort) {
        this.apiPort = apiPort;
    }

    public String getSocketAddress() {
        return socketAddress;
    }

    public void setSocketAddress(final String socketAddress) {
        this.socketAddress = socketAddress;
    }

    public int getSocketPort() {
        return socketPort;
    }

    public void setSocketPort(final int socketPort) {
        this.socketPort = socketPort;
    }

    public String getLoadBalanceAddress() {
        return loadBalanceAddress;
    }

    public void setLoadBalanceAddress(final String loadBalanceAddress) {
        this.loadBalanceAddress = loadBalanceAddress;
    }

    public int getLoadBalancePort() {
        return loadBalancePort;
    }

    public void setLoadBalancePort(final int loadBalancePort) {
        this.loadBalancePort = loadBalancePort;
    }

    public String getSiteToSiteAddress() {
        return siteToSiteAddress;
    }

    public void setSiteToSiteAddress(final String siteToSiteAddress) {
        this.siteToSiteAddress = siteToSiteAddress;
    }

    public Integer getSiteToSitePort() {
        return siteToSitePort;
    }

    public void setSiteToSitePort(final Integer siteToSitePort) {
        this.siteToSitePort = siteToSitePort;
    }

    public Integer getSiteToSiteHttpApiPort() {
        return siteToSiteHttpApiPort;
    }

    public void setSiteToSiteHttpApiPort(final Integer siteToSiteHttpApiPort) {
        this.siteToSiteHttpApiPort = siteToSiteHttpApiPort;
    }

    public Boolean getSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(final Boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    public String getNodeDn() {
        return nodeDn;
    }

    public void setNodeDn(final String nodeDn) {
        this.nodeDn = nodeDn;
    }

    public boolean isLocalNodeIdentifier() {
        return localNodeIdentifier;
    }

    public void setLocalNodeIdentifier(final boolean localNodeIdentifier) {
        this.localNodeIdentifier = localNodeIdentifier;
    }

    public static NodeIdentifierDescriptor fromNodeIdentifier(final NodeIdentifier nodeId, final boolean localNodeId) {
        final NodeIdentifierDescriptor descriptor = new NodeIdentifierDescriptor();
        descriptor.setId(nodeId.getId());
        descriptor.setApiAddress(nodeId.getApiAddress());
        descriptor.setApiPort(nodeId.getApiPort());
        descriptor.setSocketAddress(nodeId.getSocketAddress());
        descriptor.setSocketPort(nodeId.getSocketPort());
        descriptor.setSiteToSiteAddress(nodeId.getSiteToSiteAddress());
        descriptor.setSiteToSitePort(nodeId.getSiteToSitePort());
        descriptor.setSiteToSiteHttpApiPort(nodeId.getSiteToSiteHttpApiPort());
        descriptor.setSiteToSiteSecure(nodeId.isSiteToSiteSecure());
        descriptor.setNodeDn(nodeId.getDN());
        descriptor.setLoadBalanceAddress(nodeId.getLoadBalanceAddress());
        descriptor.setLoadBalancePort(nodeId.getLoadBalancePort());
        descriptor.setLocalNodeIdentifier(localNodeId);
        return descriptor;
    }

    public NodeIdentifier toNodeIdentifier() {
        return new NodeIdentifier(getId(), getApiAddress(), getApiPort(), getSocketAddress(), getSocketPort(), getLoadBalanceAddress(), getLoadBalancePort(),
            getSiteToSiteAddress(), getSiteToSitePort(), getSiteToSiteHttpApiPort(), getSiteToSiteSecure(), getNodeDn());
    }
}
