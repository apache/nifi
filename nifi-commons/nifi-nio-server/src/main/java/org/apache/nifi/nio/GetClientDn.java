package org.apache.nifi.nio;

import org.apache.nifi.security.util.CertificateUtils;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.security.cert.CertificateException;

public class GetClientDn implements MessageAction.SSLAction {

    private final IOConsumer<String> consumer;

    public GetClientDn(IOConsumer<String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public boolean execute(SSLEngine sslEngine) throws IOException {
        String clientDn = null;
        if (sslEngine != null) {
            final SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

            // If handshaking is not done yet, wait for another cycle.
            if (!handshakeStatus.equals(SSLEngineResult.HandshakeStatus.FINISHED)
                && !handshakeStatus.equals(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)) {
                return false;
            }

            try {
                clientDn = CertificateUtils.extractPeerDNFromSSLSession(sslEngine.getSession());
            } catch (CertificateException e) {
                throw new IOException("Failed to get the client DN due to " + e, e);
            }
        }
        consumer.accept(clientDn);
        return true;
    }

}
