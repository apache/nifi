package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.junit.Test;

import java.io.DataInputStream;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRequestPeerList extends AbstractS2SMessageSequenceTest {

    @Test
    public void testRequestPeerList() throws Throwable {
        testClientServerCommunication(false);
    }

    @Test
    public void testRequestPeerListSecure() throws Throwable {
        testClientServerCommunication(true);
    }

    private void testClientServerCommunication(boolean secure) throws Throwable {
        // TODO: need to test modifying peer description
        final PeerDescriptionModifier peerDescriptionModifier = null;
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new RequestPeerList(NODE_2, NODE_INFORMANT, peerDescriptionModifier, () -> CLIENT_PEER),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataInputStream dis = new DataInputStream(in);

            // Read response.
            final int numOfPeers = dis.readInt();
            assertEquals(3, numOfPeers);

            assertEquals("nifi1", dis.readUTF());
            assertEquals(8081, dis.readInt());
            assertTrue(dis.readBoolean());
            assertEquals(100, dis.readInt());

            assertEquals("nifi2", dis.readUTF());
            assertEquals(8082, dis.readInt());
            assertTrue(dis.readBoolean());
            assertEquals(200, dis.readInt());

            assertEquals("nifi4", dis.readUTF());
            assertEquals(8084, dis.readInt());
            assertTrue(dis.readBoolean());
            assertEquals(400, dis.readInt());
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, secure);
    }
}
