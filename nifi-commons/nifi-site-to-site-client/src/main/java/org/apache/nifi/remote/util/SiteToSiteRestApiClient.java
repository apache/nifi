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
package org.apache.nifi.remote.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpInetConnection;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;

public class SiteToSiteRestApiClient implements Closeable {

    private static final int RESPONSE_CODE_OK = 200;
    private static final int RESPONSE_CODE_CREATED = 201;
    private static final int RESPONSE_CODE_ACCEPTED = 202;
    private static final int RESPONSE_CODE_BAD_REQUEST = 400;
    private static final int RESPONSE_CODE_NOT_FOUND = 404;

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiClient.class);

    private String baseUrl;
    protected final SSLContext sslContext;
    protected final HttpProxy proxy;
    private final AtomicBoolean proxyAuthRequiresResend = new AtomicBoolean(false);

    private RequestConfig requestConfig;
    private CredentialsProvider credentialsProvider;
    private CloseableHttpClient httpClient;
    private CloseableHttpAsyncClient httpAsyncClient;

    private boolean compress = false;
    private long requestExpirationMillis = 0;
    private int serverTransactionTtl = 0;
    private int batchCount = 0;
    private long batchSize = 0;
    private long batchDurationMillis = 0;
    private TransportProtocolVersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);

    private String trustedPeerDn;
    private final ScheduledExecutorService ttlExtendTaskExecutor;
    private ScheduledFuture<?> ttlExtendingThread;
    private SiteToSiteRestApiClient extendingApiClient;

    private int connectTimeoutMillis;
    private int readTimeoutMillis;
    private static final Pattern HTTP_ABS_URL = Pattern.compile("^https?://.+$");

    public SiteToSiteRestApiClient(final SSLContext sslContext, final HttpProxy proxy) {
        this.sslContext = sslContext;
        this.proxy = proxy;

        ttlExtendTaskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName(Thread.currentThread().getName() + " TTLExtend");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    @Override
    public void close() throws IOException {
        stopExtendingTtl();
        closeSilently(httpClient);
        closeSilently(httpAsyncClient);
    }

    private CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            setupClient();
        }
        return httpClient;
    }

    private CloseableHttpAsyncClient getHttpAsyncClient() {
        if (httpAsyncClient == null) {
            setupAsyncClient();
        }
        return httpAsyncClient;
    }

    private RequestConfig getRequestConfig() {
        if (requestConfig == null) {
            setupRequestConfig();
        }
        return requestConfig;
    }

    private CredentialsProvider getCredentialsProvider() {
        if (credentialsProvider == null) {
            setupCredentialsProvider();
        }
        return credentialsProvider;
    }

    private void setupRequestConfig() {
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
            .setConnectionRequestTimeout(connectTimeoutMillis)
            .setConnectTimeout(connectTimeoutMillis)
            .setSocketTimeout(readTimeoutMillis);

        if (proxy != null) {
            requestConfigBuilder.setProxy(proxy.getHttpHost());
        }

        requestConfig = requestConfigBuilder.build();
    }

    private void setupCredentialsProvider() {
        credentialsProvider = new BasicCredentialsProvider();
        if (proxy != null) {
            if (!isEmpty(proxy.getUsername()) && !isEmpty(proxy.getPassword())) {
                credentialsProvider.setCredentials(
                    new AuthScope(proxy.getHttpHost()),
                    new UsernamePasswordCredentials(proxy.getUsername(), proxy.getPassword()));
            }

        }
    }

    private void setupClient() {
        final HttpClientBuilder clientBuilder = HttpClients.custom();

        if (sslContext != null) {
            clientBuilder.setSslcontext(sslContext);
            clientBuilder.addInterceptorFirst(new HttpsResponseInterceptor());
        }

        httpClient = clientBuilder
            .setDefaultCredentialsProvider(getCredentialsProvider()).build();
    }

    private void setupAsyncClient() {
        final HttpAsyncClientBuilder clientBuilder = HttpAsyncClients.custom();

        if (sslContext != null) {
            clientBuilder.setSSLContext(sslContext);
            clientBuilder.addInterceptorFirst(new HttpsResponseInterceptor());
        }

        httpAsyncClient = clientBuilder.setDefaultCredentialsProvider(getCredentialsProvider()).build();
        httpAsyncClient.start();
    }

    private class HttpsResponseInterceptor implements HttpResponseInterceptor {
        @Override
        public void process(final HttpResponse response, final HttpContext httpContext) throws HttpException, IOException {
            final HttpCoreContext coreContext = HttpCoreContext.adapt(httpContext);
            final HttpInetConnection conn = coreContext.getConnection(HttpInetConnection.class);
            if (!conn.isOpen()) {
                return;
            }

            final SSLSession sslSession;
            if (conn instanceof ManagedHttpClientConnection) {
                sslSession = ((ManagedHttpClientConnection) conn).getSSLSession();
            } else if (conn instanceof ManagedNHttpClientConnection) {
                sslSession = ((ManagedNHttpClientConnection) conn).getSSLSession();
            } else {
                throw new RuntimeException("Unexpected connection type was used, " + conn);
            }


            if (sslSession != null) {
                final Certificate[] certChain = sslSession.getPeerCertificates();
                if (certChain == null || certChain.length == 0) {
                    throw new SSLPeerUnverifiedException("No certificates found");
                }

                try {
                    final X509Certificate cert = CertificateUtils.convertAbstractX509Certificate(certChain[0]);
                    trustedPeerDn = cert.getSubjectDN().getName().trim();
                } catch (final CertificateException e) {
                    final String msg = "Could not extract subject DN from SSL session peer certificate";
                    logger.warn(msg);
                    throw new SSLPeerUnverifiedException(msg);
                }
            }
        }
    }

    public ControllerDTO getController() throws IOException {
        try {
            final HttpGet get = createGetControllerRequest();
            return execute(get, ControllerEntity.class).getController();

        } catch (final HttpGetFailedException e) {
            if (RESPONSE_CODE_NOT_FOUND == e.getResponseCode()) {
                logger.debug("getController received NOT_FOUND, trying to access the old NiFi version resource url...");
                final HttpGet get = createGet("/controller");
                return execute(get, ControllerEntity.class).getController();
            }
            throw e;
        }
    }

    private HttpGet createGetControllerRequest() {
        final HttpGet get = createGet("/site-to-site");
        get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
        return get;
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        final HttpGet get = createGet("/site-to-site/peers");
        get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
        return execute(get, PeersEntity.class).getPeers();
    }

    public String initiateTransaction(final TransferDirection direction, final String portId) throws IOException {

        final String portType = TransferDirection.RECEIVE.equals(direction) ? "output-ports" : "input-ports";
        logger.debug("initiateTransaction handshaking portType={}, portId={}", portType, portId);

        final HttpPost post = createPost("/data-transfer/" + portType + "/" + portId + "/transactions");

        post.setHeader("Accept", "application/json");
        post.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(post);

        final HttpResponse response;
        if (TransferDirection.RECEIVE.equals(direction)) {
            response = initiateTransactionForReceive(post);
        } else {
            response = initiateTransactionForSend(post);
        }

        final int responseCode = response.getStatusLine().getStatusCode();
        logger.debug("initiateTransaction responseCode={}", responseCode);

        String transactionUrl;
        switch (responseCode) {
            case RESPONSE_CODE_CREATED:
                EntityUtils.consume(response.getEntity());

                transactionUrl = readTransactionUrl(response);
                if (isEmpty(transactionUrl)) {
                    throw new ProtocolException("Server returned RESPONSE_CODE_CREATED without Location header");
                }
                final Header transportProtocolVersionHeader = response.getFirstHeader(HttpHeaders.PROTOCOL_VERSION);
                if (transportProtocolVersionHeader == null) {
                    throw new ProtocolException("Server didn't return confirmed protocol version");
                }
                final Integer protocolVersionConfirmedByServer = Integer.valueOf(transportProtocolVersionHeader.getValue());
                logger.debug("Finished version negotiation, protocolVersionConfirmedByServer={}", protocolVersionConfirmedByServer);
                transportProtocolVersionNegotiator.setVersion(protocolVersionConfirmedByServer);

                final Header serverTransactionTtlHeader = response.getFirstHeader(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL);
                if (serverTransactionTtlHeader == null) {
                    throw new ProtocolException("Server didn't return " + HttpHeaders.SERVER_SIDE_TRANSACTION_TTL);
                }
                serverTransactionTtl = Integer.parseInt(serverTransactionTtlHeader.getValue());
                break;

            default:
                try (InputStream content = response.getEntity().getContent()) {
                    throw handleErrResponse(responseCode, content);
                }
        }
        logger.debug("initiateTransaction handshaking finished, transactionUrl={}", transactionUrl);
        return transactionUrl;

    }

    /**
     * Initiate a transaction for receiving data.
     * @param post a POST request to establish transaction
     * @return POST request response
     * @throws IOException thrown if the post request failed
     */
    private HttpResponse initiateTransactionForReceive(final HttpPost post) throws IOException {
        return getHttpClient().execute(post);
    }

    /**
     * <p>
     * Initiate a transaction for sending data.
     * </p>
     *
     * <p>
     * If a proxy server requires auth, the proxy server returns 407 response with available auth schema such as basic or digest.
     * Then client has to resend the same request with its credential added.
     * This mechanism is problematic for sending data from NiFi.
     * </p>
     *
     * <p>
     * In order to resend a POST request with auth param,
     * NiFi has to either read flow-file contents to send again, or keep the POST body somewhere.
     * If we store that in memory, it would causes OOM, or storing it on disk slows down performance.
     * Rolling back processing session would be overkill.
     * Reading flow-file contents only when it's ready to send in a streaming way is ideal.
     * </p>
     *
     * <p>
     * Additionally, the way proxy authentication is done is vary among Proxy server software.
     * Some requires 407 and resend cycle for every requests, while others keep a connection between a client and
     * the proxy server, then consecutive requests skip auth steps.
     * The problem is, that how should we behave is only told after sending a request to the proxy.
     * </p>
     *
     * In order to handle above concerns correctly and efficiently, this method do the followings:
     *
     * <ol>
     * <li>Send a GET request to controller resource, to initiate an HttpAsyncClient. The instance will be used for further requests.
     *      This is not required by the Site-to-Site protocol, but it can setup proxy auth state safely.</li>
     * <li>Send a POST request to initiate a transaction. While doing so, it captures how a proxy server works.
     * If 407 and resend cycle occurs here, it implies that we need to do the same thing again when we actually send the data.
     * Because if the proxy keeps using the same connection and doesn't require an auth step, it doesn't do so here.</li>
     * <li>Then this method stores whether the final POST request should wait for the auth step.
     * So that {@link #openConnectionForSend} can determine when to produce contents.</li>
     * </ol>
     *
     * <p>
     * The above special sequence is only executed when a proxy instance is set, and its username is set.
     * </p>
     *
     * @param post a POST request to establish transaction
     * @return POST request response
     * @throws IOException thrown if the post request failed
     */
    private HttpResponse initiateTransactionForSend(final HttpPost post) throws IOException {
        if (shouldCheckProxyAuth()) {
            final CloseableHttpAsyncClient asyncClient = getHttpAsyncClient();
            final HttpGet get = createGetControllerRequest();
            final Future<HttpResponse> getResult = asyncClient.execute(get, null);
            try {
                final HttpResponse getResponse = getResult.get(readTimeoutMillis, TimeUnit.MILLISECONDS);
                logger.debug("Proxy auth check has done. getResponse={}", getResponse.getStatusLine());
            } catch (final ExecutionException e) {
                logger.debug("Something has happened at get controller requesting thread for proxy auth check. {}", e.getMessage());
                throw toIOException(e);
            } catch (TimeoutException | InterruptedException e) {
                throw new IOException(e);
            }
        }

        final HttpAsyncRequestProducer asyncRequestProducer = new HttpAsyncRequestProducer() {
            private boolean requestHasBeenReset = false;

            @Override
            public HttpHost getTarget() {
                return URIUtils.extractHost(post.getURI());
            }

            @Override
            public HttpRequest generateRequest() throws IOException, HttpException {
                final BasicHttpEntity entity = new BasicHttpEntity();
                post.setEntity(entity);
                return post;
            }

            @Override
            public void produceContent(ContentEncoder encoder, IOControl ioctrl) throws IOException {
                encoder.complete();
                if (shouldCheckProxyAuth() && requestHasBeenReset) {
                    logger.debug("Produced content again, assuming the proxy server requires authentication.");
                    proxyAuthRequiresResend.set(true);
                }
            }

            @Override
            public void requestCompleted(HttpContext context) {
                debugProxyAuthState(context);
            }

            @Override
            public void failed(Exception ex) {
                logger.error("Create transaction for {} has failed", post.getURI(), ex);
            }

            @Override
            public boolean isRepeatable() {
                return true;
            }

            @Override
            public void resetRequest() throws IOException {
                requestHasBeenReset = true;
            }

            @Override
            public void close() throws IOException {
            }
        };

        final Future<HttpResponse> responseFuture = getHttpAsyncClient().execute(asyncRequestProducer, new BasicAsyncResponseConsumer(), null);
        final HttpResponse response;
        try {
            response = responseFuture.get(readTimeoutMillis, TimeUnit.MILLISECONDS);

        } catch (final ExecutionException e) {
            logger.debug("Something has happened at initiate transaction requesting thread. {}", e.getMessage());
            throw toIOException(e);
        } catch (TimeoutException | InterruptedException e) {
            throw new IOException(e);
        }
        return response;
    }

    /**
     * Print AuthState in HttpContext for debugging purpose.
     * <p>
     * If the proxy server requires 407 and resend cycle, this method logs as followings, for Basic Auth:
     * <ul><li>state:UNCHALLENGED;</li>
     * <li>state:CHALLENGED;auth scheme:basic;credentials present</li></ul>
     * </p>
     * <p>
     * For Digest Auth:
     * <ul><li>state:UNCHALLENGED;</li>
     * <li>state:CHALLENGED;auth scheme:digest;credentials present</li></ul>
     * </p>
     * <p>
     * But if the proxy uses the same connection, it doesn't return 407, in such case
     * this method is called only once with:
     * <ul><li>state:UNCHALLENGED</li></ul>
     * </p>
     */
    private void debugProxyAuthState(HttpContext context) {
        final AuthState proxyAuthState;
        if (shouldCheckProxyAuth()
                && logger.isDebugEnabled()
                && (proxyAuthState = (AuthState)context.getAttribute("http.auth.proxy-scope")) != null){
            logger.debug("authProxyScope={}", proxyAuthState);
        }
    }

    private IOException toIOException(ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            return (IOException) cause;
        } else {
            return new IOException(cause);
        }
    }

    private boolean shouldCheckProxyAuth() {
        return proxy != null && !isEmpty(proxy.getUsername());
    }

    public boolean openConnectionForReceive(final String transactionUrl, final Peer peer) throws IOException {

        final HttpGet get = createGet(transactionUrl + "/flow-files");
        // Set uri so that it'll be used as transit uri.
        ((HttpCommunicationsSession)peer.getCommunicationsSession()).setDataTransferUrl(get.getURI().toString());

        get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(get);

        final CloseableHttpResponse response = getHttpClient().execute(get);
        final int responseCode = response.getStatusLine().getStatusCode();
        logger.debug("responseCode={}", responseCode);

        boolean keepItOpen = false;
        try {
            switch (responseCode) {
                case RESPONSE_CODE_OK:
                    logger.debug("Server returned RESPONSE_CODE_OK, indicating there was no data.");
                    EntityUtils.consume(response.getEntity());
                    return false;

                case RESPONSE_CODE_ACCEPTED:
                    final InputStream httpIn = response.getEntity().getContent();
                    final InputStream streamCapture = new InputStream() {
                        boolean closed = false;

                        @Override
                        public int read() throws IOException {
                            if (closed) {
                                return -1;
                            }
                            final int r = httpIn.read();
                            if (r < 0) {
                                closed = true;
                                logger.debug("Reached to end of input stream. Closing resources...");
                                stopExtendingTtl();
                                closeSilently(httpIn);
                                closeSilently(response);
                            }
                            return r;
                        }
                    };
                    ((HttpInput) peer.getCommunicationsSession().getInput()).setInputStream(streamCapture);

                    startExtendingTtl(transactionUrl, httpIn, response);
                    keepItOpen = true;
                    return true;

                default:
                    try (InputStream content = response.getEntity().getContent()) {
                        throw handleErrResponse(responseCode, content);
                    }
            }
        } finally {
            if (!keepItOpen) {
                response.close();
            }
        }
    }

    private final int DATA_PACKET_CHANNEL_READ_BUFFER_SIZE = 16384;
    private Future<HttpResponse> postResult;
    private CountDownLatch transferDataLatch = new CountDownLatch(1);

    public void openConnectionForSend(final String transactionUrl, final Peer peer) throws IOException {

        final CommunicationsSession commSession = peer.getCommunicationsSession();
        final String flowFilesPath = transactionUrl + "/flow-files";
        final HttpPost post = createPost(flowFilesPath);
        // Set uri so that it'll be used as transit uri.
        ((HttpCommunicationsSession)peer.getCommunicationsSession()).setDataTransferUrl(post.getURI().toString());

        post.setHeader("Content-Type", "application/octet-stream");
        post.setHeader("Accept", "text/plain");
        post.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(post);

        final CountDownLatch initConnectionLatch = new CountDownLatch(1);

        final URI requestUri = post.getURI();
        final PipedOutputStream outputStream = new PipedOutputStream();
        final PipedInputStream inputStream = new PipedInputStream(outputStream, DATA_PACKET_CHANNEL_READ_BUFFER_SIZE);
        final ReadableByteChannel dataPacketChannel = Channels.newChannel(inputStream);
        final HttpAsyncRequestProducer asyncRequestProducer = new HttpAsyncRequestProducer() {

            private final ByteBuffer buffer = ByteBuffer.allocate(DATA_PACKET_CHANNEL_READ_BUFFER_SIZE);

            private int totalRead = 0;
            private int totalProduced = 0;

            private boolean requestHasBeenReset = false;


            @Override
            public HttpHost getTarget() {
                return URIUtils.extractHost(requestUri);
            }

            @Override
            public HttpRequest generateRequest() throws IOException, HttpException {

                // Pass the output stream so that Site-to-Site client thread can send
                // data packet through this connection.
                logger.debug("sending data to {} has started...", flowFilesPath);
                ((HttpOutput) commSession.getOutput()).setOutputStream(outputStream);
                initConnectionLatch.countDown();

                final BasicHttpEntity entity = new BasicHttpEntity();
                entity.setChunked(true);
                entity.setContentType("application/octet-stream");
                post.setEntity(entity);
                return post;
            }

            private final AtomicBoolean bufferHasRemainingData = new AtomicBoolean(false);

            /**
             * If the proxy server requires authentication, the same POST request has to be sent again.
             * The first request will result 407, then the next one will be sent with auth headers and actual data.
             * This method produces a content only when it's need to be sent, to avoid producing the flow-file contents twice.
             * Whether we need to wait auth is determined heuristically by the previous POST request which creates transaction.
             * See {@link SiteToSiteRestApiClient#initiateTransactionForSend(HttpPost)} for further detail.
             */
            @Override
            public void produceContent(final ContentEncoder encoder, final IOControl ioControl) throws IOException {

                if (shouldCheckProxyAuth() && proxyAuthRequiresResend.get() && !requestHasBeenReset) {
                    logger.debug("Need authentication with proxy server. Postpone producing content.");
                    encoder.complete();
                    return;
                }

                if (bufferHasRemainingData.get()) {
                    // If there's remaining buffer last time, send it first.
                    writeBuffer(encoder);
                    if (bufferHasRemainingData.get()) {
                        return;
                    }
                }

                int read;
                // This read() blocks until data becomes available,
                // or corresponding outputStream is closed.
                if ((read = dataPacketChannel.read(buffer)) > -1) {

                    logger.trace("Read {} bytes from dataPacketChannel. {}", read, flowFilesPath);
                    totalRead += read;

                    buffer.flip();
                    writeBuffer(encoder);

                } else {

                    final long totalWritten = commSession.getOutput().getBytesWritten();
                    logger.debug("sending data to {} has reached to its end. produced {} bytes by reading {} bytes from channel. {} bytes written in this transaction.",
                            flowFilesPath, totalProduced, totalRead, totalWritten);
                    if (totalRead != totalWritten || totalProduced != totalWritten) {
                        final String msg = "Sending data to %s has reached to its end, but produced : read : wrote byte sizes (%d : %d : %d) were not equal. Something went wrong.";
                        throw new RuntimeException(String.format(msg, flowFilesPath, totalProduced, totalRead, totalWritten));
                    }
                    transferDataLatch.countDown();
                    encoder.complete();
                    dataPacketChannel.close();
                }

            }

            private void writeBuffer(ContentEncoder encoder) throws IOException {
                while (buffer.hasRemaining()) {
                    final int written = encoder.write(buffer);
                    logger.trace("written {} bytes to encoder.", written);
                    if (written == 0) {
                        logger.trace("Buffer still has remaining. {}", buffer);
                        bufferHasRemainingData.set(true);
                        return;
                    }
                    totalProduced += written;
                }
                bufferHasRemainingData.set(false);
                buffer.clear();
            }

            @Override
            public void requestCompleted(final HttpContext context) {
                logger.debug("Sending data to {} completed.", flowFilesPath);
                debugProxyAuthState(context);
            }

            @Override
            public void failed(final Exception ex) {
                logger.error("Sending data to {} has failed", flowFilesPath, ex);
            }

            @Override
            public boolean isRepeatable() {
                // In order to pass authentication, request has to be repeatable.
                return true;
            }

            @Override
            public void resetRequest() throws IOException {
                logger.debug("Sending data request to {} has been reset...", flowFilesPath);
                requestHasBeenReset = true;
            }

            @Override
            public void close() throws IOException {
                logger.debug("Closing sending data request to {}", flowFilesPath);
                closeSilently(outputStream);
                closeSilently(dataPacketChannel);
                stopExtendingTtl();
            }
        };

        postResult = getHttpAsyncClient().execute(asyncRequestProducer, new BasicAsyncResponseConsumer(), null);

        try {
            // Need to wait the post request actually started so that we can write to its output stream.
            if (!initConnectionLatch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IOException("Awaiting initConnectionLatch has been timeout.");
            }

            // Started.
            transferDataLatch = new CountDownLatch(1);
            startExtendingTtl(transactionUrl, dataPacketChannel, null);

        } catch (final InterruptedException e) {
            throw new IOException("Awaiting initConnectionLatch has been interrupted.", e);
        }

    }

    public void finishTransferFlowFiles(final CommunicationsSession commSession) throws IOException {

        if (postResult == null) {
            new IllegalStateException("Data transfer has not started yet.");
        }

        // No more data can be sent.
        // Close PipedOutputStream so that dataPacketChannel doesn't blocked.
        // If we don't close this output stream, then PipedInputStream loops infinitely at read().
        commSession.getOutput().getOutputStream().close();
        logger.debug("{} FinishTransferFlowFiles no more data can be sent", this);

        try {
            if (!transferDataLatch.await(requestExpirationMillis, TimeUnit.MILLISECONDS)) {
                throw new IOException("Awaiting transferDataLatch has been timeout.");
            }
        } catch (final InterruptedException e) {
            throw new IOException("Awaiting transferDataLatch has been interrupted.", e);
        }

        stopExtendingTtl();

        final HttpResponse response;
        try {
            response = postResult.get(readTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            logger.debug("Something has happened at sending data thread. {}", e.getMessage());
            throw toIOException(e);
        } catch (TimeoutException | InterruptedException e) {
            throw new IOException(e);
        }

        final int responseCode = response.getStatusLine().getStatusCode();
        switch (responseCode) {
            case RESPONSE_CODE_ACCEPTED:
                final String receivedChecksum = EntityUtils.toString(response.getEntity());
                ((HttpInput) commSession.getInput()).setInputStream(new ByteArrayInputStream(receivedChecksum.getBytes()));
                ((HttpCommunicationsSession) commSession).setChecksum(receivedChecksum);
                logger.debug("receivedChecksum={}", receivedChecksum);
                break;

            default:
                try (InputStream content = response.getEntity().getContent()) {
                    throw handleErrResponse(responseCode, content);
                }
        }
    }

    private void startExtendingTtl(final String transactionUrl, final Closeable stream, final CloseableHttpResponse response) {
        if (ttlExtendingThread != null) {
            // Already started.
            return;
        }
        logger.debug("Starting extending TTL thread...");
        extendingApiClient = new SiteToSiteRestApiClient(sslContext, proxy);
        extendingApiClient.transportProtocolVersionNegotiator = this.transportProtocolVersionNegotiator;
        extendingApiClient.connectTimeoutMillis = this.connectTimeoutMillis;
        extendingApiClient.readTimeoutMillis = this.readTimeoutMillis;
        final int extendFrequency = serverTransactionTtl / 2;
        ttlExtendingThread = ttlExtendTaskExecutor.scheduleWithFixedDelay(() -> {
            try {
                extendingApiClient.extendTransaction(transactionUrl);
            } catch (final Exception e) {
                logger.warn("Failed to extend transaction ttl", e);
                try {
                    // Without disconnecting, Site-to-Site client keep reading data packet,
                    // while server has already rollback.
                    this.close();
                } catch (final IOException ec) {
                    logger.warn("Failed to close", e);
                }
            }
        }, extendFrequency, extendFrequency, TimeUnit.SECONDS);
    }

    private void closeSilently(final Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final IOException e) {
            logger.warn("Got an exception during closing {}: {}", closeable, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }
    }

    public TransactionResultEntity extendTransaction(final String transactionUrl) throws IOException {
        logger.debug("Sending extendTransaction request to transactionUrl: {}", transactionUrl);

        final HttpPut put = createPut(transactionUrl);

        put.setHeader("Accept", "application/json");
        put.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(put);

        try (final CloseableHttpResponse response = getHttpClient().execute(put)) {
            final int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("extendTransaction responseCode={}", responseCode);

            try (final InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK:
                        return readResponse(content);
                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

    private void stopExtendingTtl() {
        if (!ttlExtendTaskExecutor.isShutdown()) {
            ttlExtendTaskExecutor.shutdown();
        }

        if (ttlExtendingThread != null && !ttlExtendingThread.isCancelled()) {
            logger.debug("Cancelling extending ttl...");
            ttlExtendingThread.cancel(true);
        }

        closeSilently(extendingApiClient);
    }

    private IOException handleErrResponse(final int responseCode, final InputStream in) throws IOException {
        if (in == null) {
            return new IOException("Unexpected response code: " + responseCode);
        }

        final TransactionResultEntity errEntity = readResponse(in);
        final ResponseCode errCode = ResponseCode.fromCode(errEntity.getResponseCode());

        switch (errCode) {
            case UNKNOWN_PORT:
                return new UnknownPortException(errEntity.getMessage());
            case PORT_NOT_IN_VALID_STATE:
                return new PortNotRunningException(errEntity.getMessage());
            default:
                return new IOException("Unexpected response code: " + responseCode + " errCode:" + errCode + " errMessage:" + errEntity.getMessage());
        }
    }

    private TransactionResultEntity readResponse(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        StreamUtils.copy(inputStream, bos);
        String responseMessage = null;

        try {
            responseMessage = new String(bos.toByteArray(), "UTF-8");
            logger.debug("readResponse responseMessage={}", responseMessage);

            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(responseMessage, TransactionResultEntity.class);
        } catch (JsonParseException | JsonMappingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to parse JSON.", e);
            }

            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(responseMessage);
            return entity;
        }
    }

    private String readTransactionUrl(final HttpResponse response) {
        final Header locationUriIntentHeader = response.getFirstHeader(LOCATION_URI_INTENT_NAME);
        logger.debug("locationUriIntentHeader={}", locationUriIntentHeader);

        if (locationUriIntentHeader != null && LOCATION_URI_INTENT_VALUE.equals(locationUriIntentHeader.getValue())) {
            final Header transactionUrl = response.getFirstHeader(LOCATION_HEADER_NAME);
            logger.debug("transactionUrl={}", transactionUrl);

            if (transactionUrl != null) {
                return transactionUrl.getValue();
            }
        }

        return null;
    }

    private void setHandshakeProperties(final HttpRequestBase httpRequest) {
        if (compress) {
            httpRequest.setHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION, "true");
        }

        if (requestExpirationMillis > 0) {
            httpRequest.setHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION, String.valueOf(requestExpirationMillis));
        }

        if (batchCount > 0) {
            httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_COUNT, String.valueOf(batchCount));
        }

        if (batchSize > 0) {
            httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_SIZE, String.valueOf(batchSize));
        }

        if (batchDurationMillis > 0) {
            httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_DURATION, String.valueOf(batchDurationMillis));
        }
    }

    private URI getUri(final String path) {
        final URI url;
        try {
            if (HTTP_ABS_URL.matcher(path).find()) {
                url = new URI(path);
            } else {
                if (StringUtils.isEmpty(getBaseUrl())) {
                    throw new IllegalStateException("API baseUrl is not resolved yet, call setBaseUrl or resolveBaseUrl before sending requests with relative path.");
                }
                url = new URI(baseUrl + path);
            }
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return url;
    }


    private HttpGet createGet(final String path) {
        final URI url = getUri(path);
        final HttpGet get = new HttpGet(url);
        get.setConfig(getRequestConfig());
        return get;
    }

    private HttpPost createPost(final String path) {
        final URI url = getUri(path);
        final HttpPost post = new HttpPost(url);
        post.setConfig(getRequestConfig());
        return post;
    }

    private HttpPut createPut(final String path) {
        final URI url = getUri(path);
        final HttpPut put = new HttpPut(url);
        put.setConfig(getRequestConfig());
        return put;
    }

    private HttpDelete createDelete(final String path) {
        final URI url = getUri(path);
        final HttpDelete delete = new HttpDelete(url);
        delete.setConfig(getRequestConfig());
        return delete;
    }

    private String execute(final HttpGet get) throws IOException {
        final CloseableHttpClient httpClient = getHttpClient();

        if (logger.isTraceEnabled()) {
            Arrays.stream(get.getAllHeaders()).forEach(h -> logger.debug("REQ| {}", h));
        }

        try (final CloseableHttpResponse response = httpClient.execute(get)) {
            if (logger.isTraceEnabled()) {
                Arrays.stream(response.getAllHeaders()).forEach(h -> logger.debug("RES| {}", h));
            }

            final StatusLine statusLine = response.getStatusLine();
            final int statusCode = statusLine.getStatusCode();
            if (RESPONSE_CODE_OK != statusCode) {
                throw new HttpGetFailedException(statusCode, statusLine.getReasonPhrase(), null);
            }
            final HttpEntity entity = response.getEntity();
            final String responseMessage = EntityUtils.toString(entity);
            return responseMessage;
        }
    }

    public class HttpGetFailedException extends IOException {
        private static final long serialVersionUID = 7920714957269466946L;

        private final int responseCode;
        private final String responseMessage;
        private final String explanation;

        public HttpGetFailedException(final int responseCode, final String responseMessage, final String explanation) {
            super("response code " + responseCode + ":" + responseMessage + " with explanation: " + explanation);
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
            this.explanation = explanation;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public String getDescription() {
            return !isEmpty(explanation) ? explanation : responseMessage;
        }
    }


    private <T> T execute(final HttpGet get, final Class<T> entityClass) throws IOException {
        get.setHeader("Accept", "application/json");
        final String responseMessage = execute(get);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return mapper.readValue(responseMessage, entityClass);
        } catch (JsonParseException e) {
            logger.warn("Failed to parse Json, response={}", responseMessage);
            throw e;
        }
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setConnectTimeoutMillis(final int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void setReadTimeoutMillis(final int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public String resolveBaseUrl(final String clusterUrl) {
        URI clusterUri;
        try {
            clusterUri = new URI(clusterUrl);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Specified clusterUrl was: " + clusterUrl, e);
        }
        return this.resolveBaseUrl(clusterUri);
    }

    public String resolveBaseUrl(final URI clusterUrl) {
        String urlPath = clusterUrl.getPath();
        if (urlPath.endsWith("/")) {
            urlPath = urlPath.substring(0, urlPath.length() - 1);
        }
        return resolveBaseUrl(clusterUrl.getScheme(), clusterUrl.getHost(), clusterUrl.getPort(), urlPath + "-api");
    }

    public String resolveBaseUrl(final String scheme, final String host, final int port) {
        return resolveBaseUrl(scheme, host, port, "/nifi-api");
    }

    private String resolveBaseUrl(final String scheme, final String host, final int port, final String path) {
        final String baseUri;
        try {
            baseUri = new URL(scheme, host, port, path).toURI().toString();
        } catch (MalformedURLException|URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        this.setBaseUrl(baseUri);
        return baseUri;
    }

    public void setCompress(final boolean compress) {
        this.compress = compress;
    }

    public void setRequestExpirationMillis(final long requestExpirationMillis) {
        if (requestExpirationMillis < 0) {
            throw new IllegalArgumentException("requestExpirationMillis can't be a negative value.");
        }
        this.requestExpirationMillis = requestExpirationMillis;
    }

    public void setBatchCount(final int batchCount) {
        if (batchCount < 0) {
            throw new IllegalArgumentException("batchCount can't be a negative value.");
        }
        this.batchCount = batchCount;
    }

    public void setBatchSize(final long batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize can't be a negative value.");
        }
        this.batchSize = batchSize;
    }

    public void setBatchDurationMillis(final long batchDurationMillis) {
        if (batchDurationMillis < 0) {
            throw new IllegalArgumentException("batchDurationMillis can't be a negative value.");
        }
        this.batchDurationMillis = batchDurationMillis;
    }

    public Integer getTransactionProtocolVersion() {
        return transportProtocolVersionNegotiator.getTransactionProtocolVersion();
    }

    public String getTrustedPeerDn() {
        return this.trustedPeerDn;
    }

    public TransactionResultEntity commitReceivingFlowFiles(final String transactionUrl, final ResponseCode clientResponse, final String checksum) throws IOException {
        logger.debug("Sending commitReceivingFlowFiles request to transactionUrl: {}, clientResponse={}, checksum={}",
            transactionUrl, clientResponse, checksum);

        stopExtendingTtl();

        final StringBuilder urlBuilder = new StringBuilder(transactionUrl).append("?responseCode=").append(clientResponse.getCode());
        if (ResponseCode.CONFIRM_TRANSACTION.equals(clientResponse)) {
            urlBuilder.append("&checksum=").append(checksum);
        }

        final HttpDelete delete = createDelete(urlBuilder.toString());
        delete.setHeader("Accept", "application/json");
        delete.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(delete);

        try (CloseableHttpResponse response = getHttpClient().execute(delete)) {
            final int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("commitReceivingFlowFiles responseCode={}", responseCode);

            try (InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK:
                        return readResponse(content);

                    case RESPONSE_CODE_BAD_REQUEST:
                        return readResponse(content);

                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

    public TransactionResultEntity commitTransferFlowFiles(final String transactionUrl, final ResponseCode clientResponse) throws IOException {
        final String requestUrl = transactionUrl + "?responseCode=" + clientResponse.getCode();
        logger.debug("Sending commitTransferFlowFiles request to transactionUrl: {}", requestUrl);

        final HttpDelete delete = createDelete(requestUrl);
        delete.setHeader("Accept", "application/json");
        delete.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(delete);

        try (CloseableHttpResponse response = getHttpClient().execute(delete)) {
            final int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("commitTransferFlowFiles responseCode={}", responseCode);

            try (InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK:
                        return readResponse(content);

                    case RESPONSE_CODE_BAD_REQUEST:
                        return readResponse(content);

                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

}
