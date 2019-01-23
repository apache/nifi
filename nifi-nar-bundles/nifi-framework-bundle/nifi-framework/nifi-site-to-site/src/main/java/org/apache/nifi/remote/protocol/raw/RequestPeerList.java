package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.ChainedAction;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.WriteBytes;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class RequestPeerList implements ChainedAction {

    private static final Logger LOG = LoggerFactory.getLogger(RequestPeerList.class);

    @InitialAction
    @SuppressWarnings("unused")
    private final MessageAction writePeers;

    private MessageAction nextAction;

    RequestPeerList(NodeInformation self, NodeInformant nodeInformant,
                    PeerDescriptionModifier peerDescriptionModifier, Supplier<PeerDescription> peerDescriptionSupplier) {

        writePeers = new WriteBytes(() -> {

            final PeerDescription peerDescription = peerDescriptionSupplier.get();
            LOG.debug("{} Sending Peer List to {}", this, peerDescription);

            final List<NodeInformation> clusterNodes;
            if (nodeInformant != null) {
                clusterNodes = new ArrayList<>(nodeInformant.getNodeInformation().getNodeInformation());
            } else {
                clusterNodes = Collections.singletonList(self);
            }

            // determine nodes having Site-to-site enabled
            final List<NodeInformation> nodeInfos = clusterNodes.stream()
                .filter(nodeInfo -> nodeInfo.getSiteToSitePort() != null)
                .collect(Collectors.toList());


            try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 final DataOutputStream dos = new DataOutputStream(bos)) {

                dos.writeInt(nodeInfos.size());

                for (NodeInformation nodeInfo : nodeInfos) {
                    if (peerDescriptionModifier != null && peerDescriptionModifier.isModificationNeeded(SiteToSiteTransportProtocol.RAW)) {
                        final PeerDescription target = new PeerDescription(nodeInfo.getSiteToSiteHostname(), nodeInfo.getSiteToSitePort(), nodeInfo.isSiteToSiteSecure());
                        final PeerDescription modifiedTarget = peerDescriptionModifier.modify(peerDescription, target,
                            SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.Peers, new HashMap<>());

                        dos.writeUTF(modifiedTarget.getHostname());
                        dos.writeInt(modifiedTarget.getPort());
                        dos.writeBoolean(modifiedTarget.isSecure());

                    } else {
                        dos.writeUTF(nodeInfo.getSiteToSiteHostname());
                        dos.writeInt(nodeInfo.getSiteToSitePort());
                        dos.writeBoolean(nodeInfo.isSiteToSiteSecure());
                    }

                    dos.writeInt(nodeInfo.getTotalFlowFiles());
                }

                LOG.debug("Sending list of {} peers back to client {}", nodeInfos.size(), peerDescription);
                dos.flush();
                return bos.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }, () -> nextAction = null);

        init();
    }

    @Override
    public MessageAction getNextAction() {
        return nextAction;
    }
}
