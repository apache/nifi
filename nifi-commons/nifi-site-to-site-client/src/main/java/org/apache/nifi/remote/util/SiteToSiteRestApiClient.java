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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.SiteToSiteEventReporter;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.HandshakeException;
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
import org.apache.nifi.security.cert.StandardPrincipalFormatter;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;

public class SiteToSiteRestApiClient implements Closeable {

    private static final String ACCEPT_HEADER = "Accept";
    private static final String APPLICATION_JSON = "application/json";
    private static final int DATA_PACKET_CHANNEL_READ_BUFFER_SIZE = 16384;

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private String baseUrl;
    protected final SSLContext sslContext;
    protected final HttpProxy proxy;
    private final SiteToSiteEventReporter eventReporter;

    private final HttpClient.Builder httpClientBuilder;
    private HttpClient httpClient;

    private boolean compress = false;
    private long requestExpirationMillis = 0;
    private int serverTransactionTtl = 0;
    private int batchCount = 0;
    private long batchSize = 0;
    private long batchDurationMillis = 0;
    private final TransportProtocolVersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);

    private String trustedPeerDn;
    private final ScheduledExecutorService ttlExtendTaskExecutor;
    private ScheduledFuture<?> ttlExtendingFuture;

    private int readTimeoutMillis;
    private long cacheExpirationMillis = 30000L;
    private static final Pattern HTTP_ABS_URL = Pattern.compile("^https?://.+$");

    private CompletableFuture<HttpResponse<String>> transactionFuture;

    private static final ConcurrentMap<String, RemoteGroupContents> contentsMap = new ConcurrentHashMap<>();
    private final long lastPruneTimestamp = System.currentTimeMillis();

    public SiteToSiteRestApiClient(final SSLContext sslContext, final HttpProxy proxy, final SiteToSiteEventReporter eventReporter) {
        this.sslContext = sslContext;
        this.proxy = proxy;
        this.eventReporter = eventReporter;

        final ThreadFactory threadFactory = Thread.ofVirtual().name(SiteToSiteRestApiClient.class.getSimpleName(), 1).factory();
        ttlExtendTaskExecutor = Executors.newScheduledThreadPool(1, threadFactory);

        httpClientBuilder = HttpClient.newBuilder();
        if (sslContext != null) {
            httpClientBuilder.sslContext(sslContext);
        }
        if (proxy != null && proxy.getPort() != null) {
            final InetSocketAddress proxyAddress = new InetSocketAddress(proxy.getHost(), proxy.getPort());
            final ProxySelector proxySelector = ProxySelector.of(proxyAddress);
            httpClientBuilder.proxy(proxySelector);
            httpClientBuilder.authenticator(getProxyAuthenticator());
        }
        httpClient = httpClientBuilder.build();
    }

    @Override
    public void close() throws IOException {
        stopExtendingTransaction();
        httpClient.shutdown();
        httpClient.close();
    }

    private Authenticator getProxyAuthenticator() {
        final String username = proxy.getUsername();
        final char[] password = proxy.getPassword().toCharArray();

        return new Authenticator() {
            private final PasswordAuthentication authentication = new PasswordAuthentication(username, password);

            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                final PasswordAuthentication passwordAuthentication;

                if (RequestorType.PROXY == getRequestorType()) {
                    passwordAuthentication = authentication;
                } else {
                    passwordAuthentication = null;
                }

                return passwordAuthentication;
            }
        };
    }

    /**
     * Parse the clusterUrls String, and try each URL in clusterUrls one by one to get a controller resource
     * from those remote NiFi instances until a controller is successfully returned or try out all URLs.
     * After this method execution, the base URL is set with the successful URL.
     * @param clusterUrls url of the remote NiFi instance, multiple urls can be specified in comma-separated format
     * @throws IllegalArgumentException when it fails to parse the URLs string,
     * URLs string contains multiple protocols (http and https mix),
     * or none of URL is specified.
     */
    public ControllerDTO getController(final String clusterUrls) throws IOException {
        return getController(ClusterUrlParser.parseClusterUrls(clusterUrls));
    }

    /**
     * Try each URL in clusterUrls one by one to get a controller resource
     * from those remote NiFi instances until a controller is successfully returned or try out all URLs.
     * After this method execution, the base URL is set with the successful URL.
     */
    public ControllerDTO getController(final Set<String> clusterUrls) throws IOException {
        IOException lastException = new IOException("Get Controller failed");
        for (final String clusterUrl : clusterUrls) {
            // The url may not be normalized if it passed directly without parsed with parseClusterUrls.
            setBaseUrl(ClusterUrlParser.resolveBaseUrl(clusterUrl));
            try {
                return getController();
            } catch (IOException e) {
                lastException = e;
                logger.warn("Failed to get controller from {}", clusterUrl, e);
            }
        }

        if (clusterUrls.size() > 1) {
            throw new IOException("Tried all cluster URLs but none of those was accessible. Last Exception was " + lastException, lastException);
        }
        throw lastException;
    }

    private ControllerDTO getController() throws IOException {
        // first check cache and prune any old values.
        // Periodically prune the map so that we are not keeping entries around forever, in case an RPG is removed
        // from the canvas, etc. We want to ensure that we avoid memory leaks, even if they are likely to not cause a problem.
        if (System.currentTimeMillis() > lastPruneTimestamp + TimeUnit.MINUTES.toMillis(5)) {
            pruneCache();
        }

        final String internedUrl = baseUrl.intern();
        synchronized (internedUrl) {
            final RemoteGroupContents groupContents = contentsMap.get(internedUrl);

            if (groupContents == null || groupContents.getContents() == null || groupContents.isOlderThan(cacheExpirationMillis)) {
                logger.debug("No Contents for remote group at URL {} or contents have expired; will refresh contents", internedUrl);

                final ControllerDTO refreshedContents;
                try {
                    final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(getUri("/site-to-site"));
                    refreshedContents = send(requestBuilder, ControllerEntity.class).getController();
                } catch (final Exception e) {
                    // we failed to refresh contents, but we don't want to constantly poll the remote instance, failing.
                    // So we put the ControllerDTO back but use a new RemoteGroupContents so that we get a new timestamp.
                    final ControllerDTO existingController = groupContents == null ? null : groupContents.getContents();
                    final RemoteGroupContents updatedContents = new RemoteGroupContents(existingController);
                    contentsMap.put(internedUrl, updatedContents);
                    throw e;
                }

                logger.debug("Successfully retrieved contents for remote group at URL {}", internedUrl);

                final RemoteGroupContents updatedContents = new RemoteGroupContents(refreshedContents);
                contentsMap.put(internedUrl, updatedContents);
                return refreshedContents;
            }

            logger.debug("Contents for remote group at URL {} have already been fetched and have not yet expired. Will return the cached value.", internedUrl);
            return groupContents.getContents();
        }
    }

    private void pruneCache() {
        for (final Map.Entry<String, RemoteGroupContents> entry : contentsMap.entrySet()) {
            final String url = entry.getKey();
            final RemoteGroupContents contents = entry.getValue();

            // If any entry in the map is more than 4 times as old as the refresh period,
            // then we can go ahead and remove it from the map. We use 4 * refreshMillis
            // just to ensure that we don't have any race condition with the above #getRemoteContents.
            if (contents.isOlderThan(TimeUnit.MINUTES.toMillis(5))) {
                contentsMap.remove(url, contents);
            }
        }
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(getUri("/site-to-site/peers"));
        return send(requestBuilder, PeersEntity.class).getPeers();
    }

    public String initiateTransaction(final TransferDirection direction, final String portId) throws IOException {
        final String portType = TransferDirection.RECEIVE.equals(direction) ? "output-ports" : "input-ports";
        logger.debug("initiateTransaction handshaking portType={}, portId={}", portType, portId);

        final URI uri = getUri("/data-transfer/" + portType + "/" + portId + "/transactions");
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.noBody());
        requestBuilder.setHeader(ACCEPT_HEADER, APPLICATION_JSON);

        final HttpResponse<InputStream> response;
        if (TransferDirection.RECEIVE.equals(direction)) {
            response = sendRequest(requestBuilder);
        } else {
            if (shouldCheckProxyAuth()) {
                getController();
            }
            response = sendRequest(requestBuilder);
        }

        final int responseCode = response.statusCode();
        logger.debug("initiateTransaction responseCode={}", responseCode);

        String transactionUrl;
        if (responseCode == HTTP_CREATED) {
            response.body().close();

            transactionUrl = readTransactionUrl(response);
            if (StringUtils.isEmpty(transactionUrl)) {
                throw new ProtocolException("Server returned RESPONSE_CODE_CREATED without Location header");
            }
            final Optional<String> transportProtocolVersionHeader = response.headers().firstValue(HttpHeaders.PROTOCOL_VERSION);
            if (transportProtocolVersionHeader.isEmpty()) {
                throw new ProtocolException("Transport Protocol Response Header not found");
            }
            final int protocolVersionConfirmedByServer = Integer.parseInt(transportProtocolVersionHeader.get());
            logger.debug("Finished version negotiation, protocolVersionConfirmedByServer={}", protocolVersionConfirmedByServer);
            transportProtocolVersionNegotiator.setVersion(protocolVersionConfirmedByServer);

            final Optional<String> serverTransactionTtlHeader = response.headers().firstValue(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL);
            if (serverTransactionTtlHeader.isEmpty()) {
                throw new ProtocolException("Transaction TTL Response Header not found");
            }
            serverTransactionTtl = Integer.parseInt(serverTransactionTtlHeader.get());
        } else {
            try (InputStream content = response.body()) {
                throw handleErrResponse(responseCode, content);
            }
        }

        logger.debug("initiateTransaction handshaking finished, transactionUrl={}", transactionUrl);
        return transactionUrl;
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
        return proxy != null && StringUtils.isNotEmpty(proxy.getUsername());
    }

    public boolean openConnectionForReceive(final String transactionUrl, final Peer peer) throws IOException {
        final URI uri = getUri(transactionUrl + "/flow-files");
        final HttpCommunicationsSession session = (HttpCommunicationsSession) peer.getCommunicationsSession();
        session.setDataTransferUrl(uri.toString());

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri).GET();

        final HttpResponse<InputStream> response = sendRequest(requestBuilder);
        final int responseCode = response.statusCode();
        switch (responseCode) {
            case HTTP_OK:
                logger.debug("Server returned RESPONSE_CODE_OK, indicating there was no data.");
                response.body().close();
                return false;
            case HTTP_ACCEPTED:
                final InputStream httpIn = response.body();
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
                            stopExtendingTransaction();
                            closeSilently(httpIn);
                        }
                        return r;
                    }
                };
                ((HttpInput) session.getInput()).setInputStream(streamCapture);
                startExtendingTransaction(transactionUrl);
                return true;
            default:
                try (InputStream content = response.body()) {
                    throw handleErrResponse(responseCode, content);
                }
        }
    }

    public void openConnectionForSend(final String transactionUrl, final Peer peer) throws IOException {
        final String flowFilesPath = transactionUrl + "/flow-files";
        final URI uri = getUri(flowFilesPath);
        final HttpCommunicationsSession session = (HttpCommunicationsSession) peer.getCommunicationsSession();
        session.setDataTransferUrl(uri.toString());

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);
        requestBuilder.setHeader("Content-Type", "application/octet-stream");
        requestBuilder.setHeader(ACCEPT_HEADER, "text/plain");
        requestBuilder.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
        setRequestHeaders(requestBuilder);

        final PipedOutputStream outputStream = new PipedOutputStream();
        final PipedInputStream inputStream = new PipedInputStream(outputStream, DATA_PACKET_CHANNEL_READ_BUFFER_SIZE);
        final HttpOutput httpOutput = (HttpOutput) session.getOutput();
        httpOutput.setOutputStream(outputStream);

        requestBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(
                () -> inputStream
        ));
        final HttpRequest request = requestBuilder.build();
        transactionFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        startExtendingTransaction(transactionUrl);
    }

    public void finishTransferFlowFiles(final CommunicationsSession commSession) throws IOException {
        if (transactionFuture == null) {
            throw new IllegalStateException("Data Transfer not started");
        }

        // Close PipedOutputStream from openConnectionForSend() to avoid blocking on PipedInputStream.read()
        commSession.getOutput().getOutputStream().close();

        stopExtendingTransaction();

        final HttpResponse<String> response;
        try {
            response = transactionFuture.get(readTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            throw toIOException(e);
        } catch (TimeoutException | InterruptedException e) {
            throw new IOException(e);
        }

        final int responseCode = response.statusCode();
        if (responseCode == HTTP_ACCEPTED) {
            final String receivedChecksum = response.body();
            ((HttpInput) commSession.getInput()).setInputStream(new ByteArrayInputStream(receivedChecksum.getBytes()));
            ((HttpCommunicationsSession) commSession).setChecksum(receivedChecksum);
            logger.debug("receivedChecksum={}", receivedChecksum);
        } else {
            try (InputStream content = new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))) {
                throw handleErrResponse(responseCode, content);
            }
        }
    }

    private void startExtendingTransaction(final String transactionUrl) {
        if (ttlExtendingFuture == null) {
            final int extendFrequency = serverTransactionTtl / 2;
            logger.debug("Extend Transaction Started [{}] Frequency [{} seconds]", transactionUrl, extendFrequency);
            final Runnable command = new ExtendTransactionCommand(this, transactionUrl, eventReporter);
            ttlExtendingFuture = ttlExtendTaskExecutor.scheduleWithFixedDelay(command, extendFrequency, extendFrequency, TimeUnit.SECONDS);
        }
    }

    private void closeSilently(final Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final IOException e) {
            logger.debug("Failed close [{}]", closeable, e);
        }
    }

    public TransactionResultEntity extendTransaction(final String transactionUrl) throws IOException {
        logger.debug("Sending extendTransaction request to transactionUrl: {}", transactionUrl);
        final URI uri = getUri(transactionUrl);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri).PUT(HttpRequest.BodyPublishers.noBody());
        return send(requestBuilder, TransactionResultEntity.class);
    }

    private void stopExtendingTransaction() {
        if (!ttlExtendTaskExecutor.isShutdown()) {
            ttlExtendTaskExecutor.shutdown();
        }

        if (ttlExtendingFuture != null && !ttlExtendingFuture.isCancelled()) {
            final boolean cancelled = ttlExtendingFuture.cancel(true);
            logger.debug("Extend Transaction Cancelled [{}]", cancelled);
        }
    }

    private IOException handleErrResponse(final int responseCode, final InputStream in) throws IOException {
        final TransactionResultEntity errEntity = readResponse(in);
        final ResponseCode errCode = ResponseCode.fromCode(errEntity.getResponseCode());

        return switch (errCode) {
            case UNKNOWN_PORT -> new UnknownPortException(errEntity.getMessage());
            case PORT_NOT_IN_VALID_STATE -> new PortNotRunningException(errEntity.getMessage());
            default -> {
                if (responseCode == HTTP_FORBIDDEN) {
                    yield new HandshakeException(errEntity.getMessage());
                }
                yield new IOException("Unexpected response code: " + responseCode + " errCode:" + errCode + " errMessage:" + errEntity.getMessage());
            }
        };
    }

    private TransactionResultEntity readResponse(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        StreamUtils.copy(inputStream, bos);
        String responseMessage = null;

        try {
            responseMessage = bos.toString(StandardCharsets.UTF_8);
            return objectMapper.readValue(responseMessage, TransactionResultEntity.class);
        } catch (final JsonParseException | JsonMappingException e) {
            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(responseMessage);
            return entity;
        }
    }

    private String readTransactionUrl(final HttpResponse<InputStream> response) {
        final Optional<String> locationUriIntentHeader = response.headers().firstValue(LOCATION_URI_INTENT_NAME);
        logger.debug("locationUriIntentHeader={}", locationUriIntentHeader);

        if (locationUriIntentHeader.isPresent() && LOCATION_URI_INTENT_VALUE.equals(locationUriIntentHeader.get())) {
            final Optional<String> transactionUrl = response.headers().firstValue(LOCATION_HEADER_NAME);
            logger.debug("transactionUrl={}", transactionUrl);

            if (transactionUrl.isPresent()) {
                return transactionUrl.get();
            }
        }

        return null;
    }

    private void setRequestHeaders(final HttpRequest.Builder requestBuilder) {
        if (compress) {
            requestBuilder.setHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION, Boolean.TRUE.toString());
        }

        if (requestExpirationMillis > 0) {
            requestBuilder.setHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION, String.valueOf(requestExpirationMillis));
        }

        if (batchCount > 0) {
            requestBuilder.setHeader(HANDSHAKE_PROPERTY_BATCH_COUNT, String.valueOf(batchCount));
        }

        if (batchSize > 0) {
            requestBuilder.setHeader(HANDSHAKE_PROPERTY_BATCH_SIZE, String.valueOf(batchSize));
        }

        if (batchDurationMillis > 0) {
            requestBuilder.setHeader(HANDSHAKE_PROPERTY_BATCH_DURATION, String.valueOf(batchDurationMillis));
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

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setConnectTimeoutMillis(final int connectTimeoutMillis) {
        httpClientBuilder.connectTimeout(Duration.ofMillis(connectTimeoutMillis));
        httpClient.close();
        httpClient = httpClientBuilder.build();
    }

    public void setReadTimeoutMillis(final int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public void setCacheExpirationMillis(final long expirationMillis) {
        this.cacheExpirationMillis = expirationMillis;
    }

    public void setBaseUrl(final String scheme, final String host, final int port) {
        final String baseUri;
        try {
            final URI uri = new URI(scheme, null, host, port, "/nifi-api", null, null);
            baseUri = uri.toString();
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        this.setBaseUrl(baseUri);
    }

    public void setCompress(final boolean compress) {
        this.compress = compress;
    }

    public void setLocalAddress(final InetAddress localAddress) {
        httpClientBuilder.localAddress(localAddress);
        httpClient.close();
        httpClient = httpClientBuilder.build();
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

        stopExtendingTransaction();

        final StringBuilder urlBuilder = new StringBuilder(transactionUrl).append("?responseCode=").append(clientResponse.getCode());
        if (ResponseCode.CONFIRM_TRANSACTION.equals(clientResponse)) {
            urlBuilder.append("&checksum=").append(checksum);
        }

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(getUri(urlBuilder.toString())).DELETE();
        requestBuilder.setHeader(ACCEPT_HEADER, APPLICATION_JSON);

        final HttpResponse<InputStream> response = sendRequest(requestBuilder);
        final int responseCode = response.statusCode();
        try (InputStream content = response.body()) {
            return switch (responseCode) {
                case HTTP_OK, HTTP_BAD_REQUEST -> readResponse(content);
                default -> throw handleErrResponse(responseCode, content);
            };
        }
    }

    public TransactionResultEntity commitTransferFlowFiles(final String transactionUrl, final ResponseCode clientResponse) throws IOException {
        final String requestUrl = transactionUrl + "?responseCode=" + clientResponse.getCode();
        logger.debug("Sending commitTransferFlowFiles request to transactionUrl: {}", requestUrl);

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(getUri(requestUrl)).DELETE();
        requestBuilder.setHeader(ACCEPT_HEADER, APPLICATION_JSON);

        final HttpResponse<InputStream> response = sendRequest(requestBuilder);
        final int responseCode = response.statusCode();
        try (InputStream content = response.body()) {
            return switch (responseCode) {
                case HTTP_OK, HTTP_BAD_REQUEST -> readResponse(content);
                default -> throw handleErrResponse(responseCode, content);
            };
        }
    }

    private <T> T send(final HttpRequest.Builder requestBuilder, final Class<T> responseClass) throws IOException {
        requestBuilder.setHeader(ACCEPT_HEADER, APPLICATION_JSON);
        final HttpResponse<InputStream> response = sendRequest(requestBuilder);
        final int statusCode = response.statusCode();

        try (InputStream inputStream = response.body()) {
            if (HTTP_OK == statusCode) {
                return objectMapper.readValue(inputStream, responseClass);
            } else {
                throw new IOException("Request URI [%s] HTTP %d".formatted(response.uri(), statusCode));
            }
        }
    }

    private HttpResponse<InputStream> sendRequest(final HttpRequest.Builder requestBuilder) throws IOException {
        setRequestHeaders(requestBuilder);

        requestBuilder.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
        requestBuilder.timeout(Duration.ofMillis(readTimeoutMillis));

        final HttpRequest request = requestBuilder.build();

        try {
            final HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            final Optional<SSLSession> sslSessionFound = response.sslSession();
            sslSessionFound.ifPresent(this::setTrustedPeerDn);
            return response;
        } catch (final InterruptedException e) {
            throw new IOException("Request URI [%s] interrupted".formatted(request.uri()), e);
        }
    }

    private void setTrustedPeerDn(final SSLSession sslSession) {
        try {
            final Certificate[] peerCertificates = sslSession.getPeerCertificates();
            if (peerCertificates.length == 0) {
                logger.info("Peer Certificates not found");
            } else {
                final X509Certificate peerCertificate = (X509Certificate) peerCertificates[0];
                trustedPeerDn = StandardPrincipalFormatter.getInstance().getSubject(peerCertificate);
            }
        } catch (final SSLPeerUnverifiedException e) {
            logger.warn("Peer Certificate verification failed", e);
        }
    }

    private static class RemoteGroupContents {
        private final ControllerDTO contents;
        private final long timestamp;

        public RemoteGroupContents(final ControllerDTO contents) {
            this.contents = contents;
            this.timestamp = System.currentTimeMillis();
        }

        public ControllerDTO getContents() {
            return contents;
        }

        public boolean isOlderThan(final long millis) {
            final long millisSinceRefresh = System.currentTimeMillis() - timestamp;
            return millisSinceRefresh > millis;
        }
    }
}
