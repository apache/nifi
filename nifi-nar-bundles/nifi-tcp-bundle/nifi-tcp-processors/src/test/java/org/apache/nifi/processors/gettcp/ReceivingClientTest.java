package org.apache.nifi.processors.gettcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunication";
        InetSocketAddress address = new InetSocketAddress(9999);
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
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunicationWithClientBufferSmallerThenMessage";
        InetSocketAddress address = new InetSocketAddress(9999);
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
        String msgToSend = "Hello from validateMessageSendBeforeAfterClientConnectDisconnectNoEndOfMessageByte";
        InetSocketAddress address = new InetSocketAddress(9999);
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
        String msgToSend = "Hello from validateReconnectDuringReceive\r";
        InetSocketAddress addressMain = new InetSocketAddress(9998);
        Server server = new Server(addressMain, 1024, EOM);
        server.start();

        ExecutorService sendingExecutor = Executors.newSingleThreadExecutor();

        ReceivingClient client = new ReceivingClient(addressMain, this.scheduler, 1024, EOM);
        client.setBackupAddress(addressMain);
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

    @Test
    public void validateConnectionFailureAfterRetries() throws Exception {
        ReceivingClient client = null;
        try {
            InetSocketAddress addressMain = new InetSocketAddress(9998);
            InetSocketAddress addressSecondary = new InetSocketAddress(9999);

            client = new ReceivingClient(addressMain, this.scheduler, 1024, EOM);
            client.setBackupAddress(addressSecondary);
            client.setReconnectAttempts(5);
            client.setDelayMillisBeforeReconnect(200);
            client.start();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
        assertFalse(client.isRunning());
    }

    private void sendToSocket(InetSocketAddress address, String message) throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(message);
        out.flush();
        socket.close();
    }
}
