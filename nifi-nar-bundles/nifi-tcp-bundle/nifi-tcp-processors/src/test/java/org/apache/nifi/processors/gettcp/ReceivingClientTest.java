package org.apache.nifi.processors.gettcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore // Ignored for full build due to artificial delays given the
        // multi-threaded nature of most of the tests. Please un-Ignore and run
        // when working on changes
public class ReceivingClientTest {

    private final static byte EOM = '\r';

    private ScheduledExecutorService scheduler;

    @Before
    public void before() {
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    @After
    public void after() {
        this.scheduler.shutdownNow();
    }

    @Test
    public void validateSuccessfullConnectionAndCommunication() throws Exception {
        int port = this.availablePort();
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunication";
        InetSocketAddress address = new InetSocketAddress(port);
        Server server = new Server(address, 1024, EOM);
        server.start();

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 1024, EOM);
        StringBuilder stringBuilder = new StringBuilder();
        client.setMessageHandler((fromAddress, message, partialMessage) -> stringBuilder.append(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals("", stringBuilder.toString());
        this.sendToSocket(address, "\r");
        Thread.sleep(200);
        assertEquals(msgToSend + "\r", stringBuilder.toString());

        client.stop();
        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateSuccessfullConnectionAndCommunicationWithClientBufferSmallerThenMessage() throws Exception {
        int port = this.availablePort();
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunicationWithClientBufferSmallerThenMessage";
        InetSocketAddress address = new InetSocketAddress(port);
        Server server = new Server(address, 1024, EOM);
        server.start();

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 64, EOM);
        List<String> messages = new ArrayList<>();
        client.setMessageHandler((fromAddress, message, partialMessage) -> messages.add(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        this.sendToSocket(address, "\r");
        Thread.sleep(200);
        assertEquals("Hello from validateSuccessfullConnectionAndCommunicationWithClie", messages.get(0));
        assertEquals("ntBufferSmallerThenMessage\r", messages.get(1));

        client.stop();
        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateMessageSendBeforeAfterClientConnectDisconnectNoEndOfMessageByte() throws Exception {
        int port = this.availablePort();
        String msgToSend = "Hello from validateMessageSendBeforeAfterClientConnectDisconnectNoEndOfMessageByte";
        InetSocketAddress address = new InetSocketAddress(port);
        Server server = new Server(address, 1024, EOM);
        server.start();
        this.sendToSocket(address, "foo"); // validates no unexpected errors

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 30, EOM);
        List<String> messages = new ArrayList<>();
        client.setMessageHandler((fromAddress, message, partialMessage) -> messages.add(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals(2, messages.size());
        assertEquals("Hello from validateMessageSend", messages.get(0));
        assertEquals("BeforeAfterClientConnectDiscon", messages.get(1));
        messages.clear();

        client.stop();
        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals(0, messages.size());

        this.sendToSocket(address, msgToSend);

        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateReconnectDuringReceive() throws Exception {
        int port = this.availablePort();
        String msgToSend = "Hello from validateReconnectDuringReceive\r";
        InetSocketAddress addressMain = new InetSocketAddress(port);
        Server server = new Server(addressMain, 1024, EOM);
        server.start();

        ExecutorService sendingExecutor = Executors.newSingleThreadExecutor();

        ReceivingClient client = new ReceivingClient(addressMain, this.scheduler, 1024, EOM);
        client.setReconnectAttempts(10);
        client.setDelayMillisBeforeReconnect(1000);
        client.setMessageHandler((fromAddress, message, partialMessage) -> System.out.println(new String(message)));
        client.start();
        assertTrue(client.isRunning());

        sendingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    try {
                        sendToSocket(addressMain, msgToSend);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ex) {
                            // ignore
                        }
                    }

                }
            }
        });

        Thread.sleep(500);
        server.stop();

        Thread.sleep(500);

        server.start();
        Thread.sleep(1000);

        client.stop();
        server.stop();

        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    private void sendToSocket(InetSocketAddress address, String message) throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(message);
        out.flush();
        socket.close();
    }

    /**
     * Will determine the available port used by test server.
     */
    private int availablePort() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to discover available port.", e);
        } finally {
            try {
                s.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
