package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.ChainedAction;
import org.apache.nifi.nio.GetClientDn;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.ReadInt;
import org.apache.nifi.nio.ReadUTF;
import org.apache.nifi.nio.WriteBytes;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class Handshake implements ChainedAction {

    private static final Logger LOG = LoggerFactory.getLogger(Handshake.class);

    @InitialAction
    @SuppressWarnings("unused")
    private final MessageAction getClientDn;
    private final MessageAction readCommunicationId;
    private final MessageAction readPropertyValue;
    private final MessageAction readPropertyCount;
    private final MessageAction readTransitUriPrefix;
    private MessageAction readPropertyKey;

    @AutoCleared
    private String clientDn;
    @AutoCleared
    private HandshakeProperties handshakeProperties;
    @AutoCleared
    private Map<String, String> properties;
    @AutoCleared
    private Integer remainingPropertiesToRead;
    @AutoCleared
    private String propertyKey;

    @AutoCleared
    private ResponseCode handshakeResult;
    @AutoCleared
    private String handshakeResultMessage;
    @AutoCleared
    private HandshakeException handshakeException;

    private MessageAction nextAction;

    public Handshake(Supplier<ServerProtocol> protocolSupplier) {

        readPropertyCount = new ReadInt(count -> {
            LOG.debug("Read propertyCount={}", count);
            remainingPropertiesToRead = count;
            properties = new HashMap<>(count);
            if (remainingPropertiesToRead > 0) {
                nextAction = readPropertyKey;
            } else {
                validateHandshakeProperties(protocolSupplier);
            }
        });

        readTransitUriPrefix = new ReadUTF(prefix -> {
            LOG.debug("Read readTransitUriPrefix={}", prefix);
            String transitUriPrefix = prefix;
            if (!transitUriPrefix.endsWith("/")) {
                transitUriPrefix = transitUriPrefix + "/";
            }
            handshakeProperties.setTransitUriPrefix(transitUriPrefix);
            nextAction = readPropertyCount;
        });

        readCommunicationId = new ReadUTF(id -> {
            LOG.debug("Read communicationId={}", id);
            handshakeProperties = new HandshakeProperties();
            handshakeProperties.setCommsIdentifier(id);

            final int protocolVersion = protocolSupplier.get().getVersionNegotiator().getVersion();
            if (protocolVersion >= 3) {
                nextAction = readTransitUriPrefix;
            } else {
                nextAction = readPropertyCount;
            }

        });

        getClientDn = new GetClientDn(dn -> {
            clientDn = dn;
            nextAction = readCommunicationId;
        });

        readPropertyValue = new ReadUTF(value -> {
            LOG.debug("Read propertyValue={}", value);
            properties.put(propertyKey, value);
            remainingPropertiesToRead--;
            nextAction = this.readNextProperty();
            if (remainingPropertiesToRead > 0) {
                nextAction = this.readNextProperty();
            } else {
                validateHandshakeProperties(protocolSupplier);
            }
        });

        readPropertyKey = new ReadUTF(key -> {
            LOG.debug("Read propertyKey={}", key);
            propertyKey = key;
            nextAction = readPropertyValue;
        });

        init();
    }

    private void validateHandshakeProperties(Supplier<ServerProtocol> protocolSupplier) {
        final NIOSocketFlowFileServerProtocol protocol = (NIOSocketFlowFileServerProtocol) protocolSupplier.get();
        try {
            protocol.validateHandshakeRequest(handshakeProperties, clientDn, properties);
            protocol.setHandshakeProperties(handshakeProperties);
            handshakeResult = ResponseCode.PROPERTIES_OK;
        } catch (HandshakeException e) {
            handshakeException = e;
            handshakeResult = e.getResponseCode();
            handshakeResultMessage = e.getMessage();
        }
        nextAction = writeResponseCode;
    }

    private MessageAction writeResponseCode = new WriteBytes(() -> {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(bos)) {

            if (handshakeResult.containsMessage()) {
                handshakeResult.writeResponse(dos, handshakeResultMessage);
            } else {
                handshakeResult.writeResponse(dos);
            }

            dos.flush();
            return bos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }, () -> {
        nextAction = null;
    });


    private MessageAction readNextProperty() {
        readPropertyKey.clear();
        readPropertyValue.clear();
        return readPropertyKey;
    }


    @Override
    public MessageAction getNextAction() {
        return nextAction;
    }

    public String getClientDn() {
        return clientDn;
    }
}
