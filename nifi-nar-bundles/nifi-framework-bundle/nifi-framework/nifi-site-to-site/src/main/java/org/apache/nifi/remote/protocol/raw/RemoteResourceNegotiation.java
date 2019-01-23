package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.ChainedAction;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.ReadInt;
import org.apache.nifi.nio.ReadUTF;
import org.apache.nifi.nio.WriteBytes;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.VersionedRemoteResource;
import org.apache.nifi.remote.exception.HandshakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.apache.nifi.remote.RemoteResourceInitiator.ABORT;
import static org.apache.nifi.remote.RemoteResourceInitiator.DIFFERENT_RESOURCE_VERSION;
import static org.apache.nifi.remote.RemoteResourceInitiator.RESOURCE_OK;

public class RemoteResourceNegotiation implements ChainedAction {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteResourceNegotiation.class);

    @AutoCleared
    private String resourceName;
    // Do not clear this resource so that subsequent sequences can refer.
    private VersionedRemoteResource resource;

    private MessageAction nextAction;

    @InitialAction
    @SuppressWarnings("unused")
    private final MessageAction readProtocolName;

    private final MessageAction writeAbort;
    private final MessageAction readProtocolVersion;
    private final MessageAction writeDifferentVersion = new WriteBytes(() -> new byte[]{
        Integer.valueOf(DIFFERENT_RESOURCE_VERSION).byteValue(),
        Integer.valueOf(resource.getVersionNegotiator().getPreferredVersion()).byteValue()
    }, this::negotiateDifferentVersion);

    private final MessageAction writeResourceOk = new WriteBytes(() -> new byte[]{Integer.valueOf(RESOURCE_OK).byteValue()}, () -> {
        LOG.debug("Resource negotiation completed. {} ver {}", resourceName, resource.getVersionNegotiator().getVersion());
        nextAction = null;
    });

    RemoteResourceNegotiation(String resourceType, Function<String, VersionedRemoteResource> createResource) {

        writeAbort = new WriteBytes(() -> new byte[]{Integer.valueOf(ABORT).byteValue()}, () -> {
            nextAction = null;
            throw new HandshakeException("Unable to negotiate an acceptable version of the " + resourceType + " " + resourceName);
        });

        readProtocolVersion = new ReadInt(version -> {
            LOG.debug("Received protocol version {}", version);
            final VersionNegotiator negotiator = resource.getVersionNegotiator();
            if (negotiator.isVersionSupported(version)) {
                LOG.debug("Negotiated protocol version {}", version);
                negotiator.setVersion(version);
                nextAction = writeResourceOk;

            } else {
                final Integer preferredProtocolVersion = negotiator.getPreferredVersion(version);
                if (preferredProtocolVersion == null) {
                    nextAction = writeAbort;
                    return;
                }

                LOG.debug("Negotiated another protocol version {}", preferredProtocolVersion);
                nextAction = writeDifferentVersion;
            }
        });

        readProtocolName = new ReadUTF(name -> {
            LOG.debug("Negotiating resource {} {}", resourceType, name);
            resourceName = name;
            resource = createResource.apply(name);
            nextAction = readProtocolVersion;
        });

        init();
    }


    private void negotiateDifferentVersion() {
        readProtocolVersion.clear();
        nextAction = readProtocolVersion;
    }

    @Override
    public MessageAction getNextAction() {
        return nextAction;
    }

    public VersionedRemoteResource getResource() {
        return resource;
    }
}
