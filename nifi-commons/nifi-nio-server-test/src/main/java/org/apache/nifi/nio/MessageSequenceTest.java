package org.apache.nifi.nio;

import org.apache.nifi.security.util.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Testing framework for NIO message sequences with real network communications between a server and clients.
 */
public class MessageSequenceTest {

    private static final Logger log = LoggerFactory.getLogger(MessageSequenceTest.class);


    private static Socket createClientSocket(InetSocketAddress serverAddress, SSLContext sslContext) throws IOException {
        if (sslContext != null) {
            return sslContext.getSocketFactory().createSocket(serverAddress.getAddress(), serverAddress.getPort());
        } else {
            return new Socket(serverAddress.getAddress(), serverAddress.getPort());
        }
    }

    private static Runnable hookThread(InetSocketAddress serverAddress, ServerStarted hook, Consumer<Throwable> onError) {
        return () -> {
            try {
                hook.getOnStarted().accept(serverAddress);
            } catch (Throwable e) {
                onError.accept(e);
            }
        };
    }

    @FunctionalInterface
    public interface MessageSequenceHandlerProvider {
        MessageSequenceHandler get(InetSocketAddress serverAddress, CountDownLatch latch, Consumer<Throwable> errorHandler);
    }

    /**
     * This function represents a partial client logic that directly execute Socket IO to the raw Socket streams.
     */
    @FunctionalInterface
    public interface ClientRunnable {
        void run(InputStream in, OutputStream out) throws IOException;
    }

    @FunctionalInterface
    public interface OnServerStarted {
        void accept(InetSocketAddress socketAddress) throws IOException;
    }

    public static class ServerStarted {
        private final String name;
        private final OnServerStarted onStarted;

        public ServerStarted(String name, OnServerStarted onStarted) {
            this.name = name;
            this.onStarted = onStarted;
        }

        public String getName() {
            return name;
        }

        public OnServerStarted getOnStarted() {
            return onStarted;
        }
    }

    public static void testMessageSequence(MessageSequenceHandlerProvider sequenceHandlerProvider,
                                           ServerStarted... hooks) throws Throwable {

        testMessageSequence(sequenceHandlerProvider, hooks, new ClientRunnable[0]);
    }

    public static void testMessageSequence(MessageSequenceHandlerProvider sequenceHandlerProvider,
                                           ClientRunnable... clients) throws Throwable {
        testMessageSequence(sequenceHandlerProvider, new ServerStarted[0], clients);
    }

    public static void testMessageSequence(MessageSequenceHandlerProvider sequenceHandlerProvider,
                                           ServerStarted[] hooks,
                                           ClientRunnable[] clients) throws Throwable {
        testMessageSequence(sequenceHandlerProvider, hooks, clients, false);
    }

    public static void testMessageSequence(MessageSequenceHandlerProvider sequenceHandlerProvider,
                                           ServerStarted[] hooks,
                                           ClientRunnable[] clients,
                                           boolean secure) throws Throwable {
        if (secure) {

            final SSLContext serverSSLContext = createSSLContext(
                MessageSequenceTest.class.getResource("/localhost/truststore.jks").getFile(),
                "password",
                MessageSequenceTest.class.getResource("/localhost/keystore.jks").getFile(),
                "password"
            );

            final SSLContext clientSSLContext = createSSLContext(
                MessageSequenceTest.class.getResource("/client/truststore.jks").getFile(),
                "password",
                MessageSequenceTest.class.getResource("/client/keystore.jks").getFile(),
                "password"
            );

            testMessageSequence(sequenceHandlerProvider, hooks, clients, serverSSLContext, clientSSLContext);

        } else {
            testMessageSequence(sequenceHandlerProvider, hooks, clients, null, null);
        }
    }

    public static void testMessageSequence(MessageSequenceHandlerProvider messageSequenceHandlerProvider,
                                           ServerStarted[] hooks,
                                           ClientRunnable[] clients,
                                           SSLContext serverSSLContext,
                                           SSLContext clientSSLContext) throws Throwable {
        try (final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(0));

            final int numOfActors = clients.length + 1;
            final CountDownLatch latch = new CountDownLatch(numOfActors);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final Consumer<Throwable> errorHandler = e -> {
                error.set(e);
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            };

            final InetSocketAddress serverAddress = (InetSocketAddress) serverSocketChannel.getLocalAddress();

            final MessageSequenceHandler sequenceHandler = messageSequenceHandlerProvider.get(serverAddress, latch, errorHandler);

            final List<Thread> threads = new ArrayList<>(numOfActors);
            threads.add(new Thread(() -> {
                try {
                    sequenceHandler.start(serverSocketChannel, serverSSLContext);
                } catch (Throwable e) {
                    errorHandler.accept(e);
                }
            }, "NIO-Server"));


            for (ServerStarted hook : hooks) {
                threads.add(new Thread(hookThread(serverAddress, hook, errorHandler), hook.getName()));
            }

            for (int i = 0; i < clients.length; i++) {
                final ClientRunnable client = clients[i];
                threads.add(new Thread(() -> {
                    try (final Socket socket = createClientSocket(serverAddress, clientSSLContext)) {

                        final InputStream in = socket.getInputStream();
                        final OutputStream out = socket.getOutputStream();
                        client.run(in, out);
                        latch.countDown();

                    } catch (Throwable e) {
                        errorHandler.accept(e);
                    }
                }, "NIO-Client-" + i));
            }

            for (Thread thread : threads) {
                thread.start();
            }

            latch.await(10, TimeUnit.MINUTES);
            sequenceHandler.stop();

            for (Thread thread : threads) {
                thread.join(3_000);
            }

            // If each thread finishes too fast, the captured error is not set yet.
            // Wait for a little while to prevent that.
            Thread.sleep(10);
            if (error.get() != null) {
                throw error.get();
            }

        }
    }

    private static SSLContext createSSLContext(String trustStoreFile, String trustStorePassword,
                                        String keyStoreFile, String keyStorePassword)
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {


        return SslContextFactory.createSslContext(keyStoreFile, keyStorePassword.toCharArray(), "JKS",
            trustStoreFile, trustStorePassword.toCharArray(), "JKS",
            SslContextFactory.ClientAuth.REQUIRED, "TLS");
    }
}
