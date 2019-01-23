package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHandshake extends AbstractS2SMessageSequenceTest {

    @Test
    public void testHandshake() throws Throwable {
        testSuccessfulHandshake(false);
    }

    @Test
    public void testHandshakeSecure() throws Throwable {
        testSuccessfulHandshake(true);
    }

    private void testSuccessfulHandshake(boolean secure) throws Throwable {

        final NIOSocketFlowFileServerProtocol serverProtocol = Mockito.spy(new NIOSocketFlowFileServerProtocol());
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(true);
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(true);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getInputPort(eq("input-port-1"))).thenReturn(port);
        serverProtocol.setRootProcessGroup(processGroup);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new Handshake(() -> serverProtocol),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataInputStream din = new DataInputStream(in);
            final DataOutputStream dis = new DataOutputStream(out);

            // Communication Id
            dis.writeUTF("comm-123");

            // Transaction URI prefix
            dis.writeUTF("tx-uri-prefix");

            // Num of handshake properties
            dis.writeInt(3);

            // Write properties.
            dis.writeUTF("PORT_IDENTIFIER");
            dis.writeUTF("input-port-1");
            dis.writeUTF("REQUEST_EXPIRATION_MILLIS");
            dis.writeUTF("30000");
            dis.writeUTF("GZIP");
            dis.writeUTF("false");

            // Read response.
            final Response response = Response.read(din);
            assertEquals(ResponseCode.PROPERTIES_OK, response.getCode());
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, secure);
    }

    @Test
    public void testMissingRequiredProperties() throws Throwable {

        final NIOSocketFlowFileServerProtocol serverProtocol = Mockito.spy(new NIOSocketFlowFileServerProtocol());
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(true);
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(true);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getInputPort(eq("input-port-1"))).thenReturn(port);
        serverProtocol.setRootProcessGroup(processGroup);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new Handshake(() -> serverProtocol),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataInputStream din = new DataInputStream(in);
            final DataOutputStream dis = new DataOutputStream(out);

            // Communication Id
            dis.writeUTF("comm-123");

            // Transaction URI prefix
            dis.writeUTF("tx-uri-prefix");

            // Num of handshake properties
            dis.writeInt(0);

            // Read response.
            final Response response = Response.read(din);
            assertEquals(ResponseCode.MISSING_PROPERTY, response.getCode());
            assertEquals("Missing Property GZIP", response.getMessage());
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, false);
    }

    @Test
    public void testNotAuthorized() throws Throwable {

        final NIOSocketFlowFileServerProtocol serverProtocol = Mockito.spy(new NIOSocketFlowFileServerProtocol());
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(false);
        when(authorizationResult.getExplanation()).thenReturn("Authorization error");
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(true);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getInputPort(eq("input-port-1"))).thenReturn(port);
        serverProtocol.setRootProcessGroup(processGroup);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new Handshake(() -> serverProtocol),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataInputStream din = new DataInputStream(in);
            final DataOutputStream dis = new DataOutputStream(out);

            // Communication Id
            dis.writeUTF("comm-123");

            // Transaction URI prefix
            dis.writeUTF("tx-uri-prefix");

            // Num of handshake properties
            dis.writeInt(3);

            // Write properties.
            dis.writeUTF("PORT_IDENTIFIER");
            dis.writeUTF("input-port-1");
            dis.writeUTF("REQUEST_EXPIRATION_MILLIS");
            dis.writeUTF("30000");
            dis.writeUTF("GZIP");
            dis.writeUTF("false");

            // Read response.
            final Response response = Response.read(din);
            assertEquals(ResponseCode.UNAUTHORIZED, response.getCode());
            assertEquals("Authorization error", response.getMessage());
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, false);
    }

    @Test
    public void testInvalidPort() throws Throwable {

        final NIOSocketFlowFileServerProtocol serverProtocol = Mockito.spy(new NIOSocketFlowFileServerProtocol());
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authorizationResult = mock(PortAuthorizationResult.class);

        when(authorizationResult.isAuthorized()).thenReturn(true);
        when(port.checkUserAuthorization(anyString())).thenReturn(authorizationResult);
        when(port.isValid()).thenReturn(false);
        when(port.isRunning()).thenReturn(true);
        when(processGroup.isRootGroup()).thenReturn(true);
        when(processGroup.getInputPort(eq("input-port-1"))).thenReturn(port);
        serverProtocol.setRootProcessGroup(processGroup);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new Handshake(() -> serverProtocol),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataInputStream din = new DataInputStream(in);
            final DataOutputStream dis = new DataOutputStream(out);

            // Communication Id
            dis.writeUTF("comm-123");

            // Transaction URI prefix
            dis.writeUTF("tx-uri-prefix");

            // Num of handshake properties
            dis.writeInt(3);

            // Write properties.
            dis.writeUTF("PORT_IDENTIFIER");
            dis.writeUTF("input-port-1");
            dis.writeUTF("REQUEST_EXPIRATION_MILLIS");
            dis.writeUTF("30000");
            dis.writeUTF("GZIP");
            dis.writeUTF("false");

            // Read response.
            final Response response = Response.read(din);
            assertEquals(ResponseCode.PORT_NOT_IN_VALID_STATE, response.getCode());
            assertEquals("Port is not valid", response.getMessage());
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, false);
    }

}
