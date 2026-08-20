/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processors.standard.ssh.StandardSshClientProvider;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FetchSFTPProxyTest {
    private static final String PROXY_SERVICE_ID = "proxy-configuration-service";

    private static final String RESOLVABLE_HOSTNAME = "localhost";

    private static final String UNRESOLVABLE_HOSTNAME = "sftp.example.invalid";

    private static final int DESTINATION_PORT = 2222;

    private static final String HTTP_CONNECT_REQUEST = "CONNECT %s:%d HTTP/1.1".formatted(UNRESOLVABLE_HOSTNAME, DESTINATION_PORT);

    private static final String USERNAME = "nifi";

    private static final String PASSWORD = UUID.randomUUID().toString();

    private static final String TIMEOUT = "5 secs";

    private static final int REQUEST_TIMEOUT_SECONDS = 15;

    private static final int SOCKS_ADDRESS_TYPE_DOMAIN = 0x03;

    private static final int SOCKS_ADDRESS_TYPE_IPV4 = 0x01;

    private static final byte[] SOCKS_NO_AUTHENTICATION_SELECTED = new byte[]{0x05, 0x00};

    private static final char CARRIAGE_RETURN = '\r';

    private static final char NEW_LINE = '\n';

    private ServerSocket proxyServer;

    private ExecutorService executorService;

    @BeforeEach
    void setProxyServer() throws IOException {
        proxyServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void closeProxyServer() throws IOException {
        executorService.shutdownNow();
        proxyServer.close();
    }

    @Test
    void testSocksProxyDestinationHostname() throws Exception {
        final Future<SocksRequest> requestFuture = executorService.submit(this::readSocksRequest);

        connect(Proxy.Type.SOCKS, RESOLVABLE_HOSTNAME);

        final SocksRequest socksRequest = requestFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(SOCKS_ADDRESS_TYPE_DOMAIN, socksRequest.addressType());
        assertEquals(RESOLVABLE_HOSTNAME, socksRequest.address());
        assertEquals(DESTINATION_PORT, socksRequest.port());
    }

    @Test
    void testSocksProxyDestinationHostnameNotResolvable() throws Exception {
        final Future<SocksRequest> requestFuture = executorService.submit(this::readSocksRequest);

        connect(Proxy.Type.SOCKS, UNRESOLVABLE_HOSTNAME);

        final SocksRequest socksRequest = requestFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(SOCKS_ADDRESS_TYPE_DOMAIN, socksRequest.addressType());
        assertEquals(UNRESOLVABLE_HOSTNAME, socksRequest.address());
        assertEquals(DESTINATION_PORT, socksRequest.port());
    }

    @Test
    void testHttpProxyDestinationHostnameNotResolvable() throws Exception {
        final Future<String> requestFuture = executorService.submit(this::readHttpConnectRequestLine);

        connect(Proxy.Type.HTTP, UNRESOLVABLE_HOSTNAME);

        final String requestLine = requestFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(HTTP_CONNECT_REQUEST, requestLine);
    }

    private void connect(final Proxy.Type proxyType, final String hostname) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(FetchSFTP.class);
        runner.setProperty(FileTransfer.HOSTNAME, hostname);
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(DESTINATION_PORT));
        runner.setProperty(FileTransfer.USERNAME, USERNAME);
        runner.setProperty(SFTPTransfer.PASSWORD, PASSWORD);
        runner.setProperty(SFTPTransfer.CONNECTION_TIMEOUT, TIMEOUT);
        runner.setProperty(SFTPTransfer.DATA_TIMEOUT, TIMEOUT);
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, Boolean.FALSE.toString());

        final SimpleProxyConfigurationService service = new SimpleProxyConfigurationService(proxyType, proxyServer.getLocalPort());
        runner.addControllerService(PROXY_SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(SFTPTransfer.PROXY_CONFIGURATION_SERVICE, PROXY_SERVICE_ID);

        final StandardSshClientProvider provider = new StandardSshClientProvider();
        try {
            provider.getClientSession(runner.getProcessContext(), Map.of());
        } catch (final RuntimeException ignored) {
            // Connection failures are expected without a complete SSH server implementation
        }
    }

    private SocksRequest readSocksRequest() throws IOException {
        try (Socket socket = proxyServer.accept()) {
            final DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            final OutputStream outputStream = socket.getOutputStream();

            inputStream.readUnsignedByte();
            final int methodCount = inputStream.readUnsignedByte();
            inputStream.skipBytes(methodCount);

            outputStream.write(SOCKS_NO_AUTHENTICATION_SELECTED);
            outputStream.flush();

            // Read Version, Command, and Reserved fields preceding the address
            inputStream.readUnsignedByte();
            inputStream.readUnsignedByte();
            inputStream.readUnsignedByte();

            final int addressType = inputStream.readUnsignedByte();
            final String address;
            if (SOCKS_ADDRESS_TYPE_DOMAIN == addressType) {
                final int addressLength = inputStream.readUnsignedByte();
                final byte[] domainName = new byte[addressLength];
                inputStream.readFully(domainName);
                address = new String(domainName, StandardCharsets.US_ASCII);
            } else if (SOCKS_ADDRESS_TYPE_IPV4 == addressType) {
                final byte[] octets = new byte[4];
                inputStream.readFully(octets);
                address = InetAddress.getByAddress(octets).getHostAddress();
            } else {
                address = null;
            }

            final int port = inputStream.readUnsignedShort();
            return new SocksRequest(addressType, address, port);
        }
    }

    private String readHttpConnectRequestLine() throws IOException {
        try (Socket socket = proxyServer.accept()) {
            final DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            final StringBuilder requestLine = new StringBuilder();

            int character = inputStream.read();
            while (character != -1 && character != CARRIAGE_RETURN && character != NEW_LINE) {
                requestLine.append((char) character);
                character = inputStream.read();
            }

            return requestLine.toString();
        }
    }

    private record SocksRequest(int addressType, String address, int port) {
    }

    private static class SimpleProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {
        private final Proxy.Type proxyType;

        private final int proxyPort;

        private SimpleProxyConfigurationService(final Proxy.Type proxyType, final int proxyPort) {
            this.proxyType = proxyType;
            this.proxyPort = proxyPort;
        }

        @Override
        public ProxyConfiguration getConfiguration() {
            final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
            proxyConfiguration.setProxyType(proxyType);
            proxyConfiguration.setProxyServerHost(InetAddress.getLoopbackAddress().getHostAddress());
            proxyConfiguration.setProxyServerPort(proxyPort);
            return proxyConfiguration;
        }
    }
}
