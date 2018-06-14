package org.apache.nifi.cluster.coordination;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface ClusterTopologyEventListener {

    void onNodeAdded(NodeIdentifier nodeId);

    void onNodeRemoved(NodeIdentifier nodeId);

    void onLocalNodeIdentifierSet(NodeIdentifier localNodeId);
}
