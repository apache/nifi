package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.ReadBytes;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class VerifyMagicBytes implements MessageSequence {

    private static final Logger LOG = LoggerFactory.getLogger(VerifyMagicBytes.class);

    @AutoCleared
    private boolean verified;
    // TODO: SET
    @AutoCleared
    private String peerDescription;

    private final MessageAction verifyMagicBytes = new ReadBytes(CommunicationsSession.MAGIC_BYTES.length, bytes -> {
        // ensure that we are communicating with another NiFi
        LOG.debug("Verifying magic bytes...");
        if (!Arrays.equals(CommunicationsSession.MAGIC_BYTES, bytes)) {
            throw new HandshakeException("Handshake with " + peerDescription + " failed because the Magic Header was not present");
        }
        verified = true;
    });

    @Override
    public MessageAction getNextAction() {
        return verified ? null : verifyMagicBytes;
    }
}
