package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.junit.Test;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;

public class TestVerifyMagicBytes extends AbstractS2SMessageSequenceTest {

    @Test
    public void testClientServerCommunication() throws Throwable {
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            VerifyMagicBytes::new,
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            // Magic bytes
            out.write(CommunicationsSession.MAGIC_BYTES);
        };
        testMessageSequence(handlerProvider, client);
    }
}
