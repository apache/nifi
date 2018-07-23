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
package org.apache.nifi.remote.client;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.HttpClient;
import org.apache.nifi.remote.client.socket.SocketClient;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.security.util.KeyStoreUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * The SiteToSiteClient provides a mechanism for sending data to a remote
 * instance of NiFi (or NiFi cluster) and retrieving data from a remote instance
 * of NiFi (or NiFi cluster).
 * </p>
 *
 * <p>
 * When configuring the client via the {@link SiteToSiteClient.Builder}, the
 * Builder must be provided the URL of the remote NiFi instance. If the URL
 * points to a standalone instance of NiFi, all interaction will take place with
 * that instance of NiFi. However, if the URL points to the NiFi Cluster Manager
 * of a cluster, the client will automatically handle load balancing the
 * transactions across the different nodes in the cluster.
 * </p>
 *
 * <p>
 * The SiteToSiteClient provides a {@link Transaction} through which all
 * interaction with the remote instance takes place. After data has been
 * exchanged or it is determined that no data is available, the Transaction can
 * then be canceled (via the {@link Transaction#cancel(String)} method) or can
 * be completed (via the {@link Transaction#complete(boolean)} method).
 * </p>
 *
 * <p>
 * An instance of SiteToSiteClient can be obtained by constructing a new
 * instance of the {@link SiteToSiteClient.Builder} class, calling the
 * appropriate methods to configured the client as desired, and then calling the
 * {@link SiteToSiteClient.Builder#build() build()} method.
 * </p>
 *
 * <p>
 * The SiteToSiteClient itself is immutable once constructed and is thread-safe.
 * Many threads can share access to the same client. However, the
 * {@link Transaction} that is created by the client is not thread safe and
 * should not be shared among threads.
 * </p>
 */
public interface SiteToSiteClient extends Closeable {

    /**
     * <p>
     * Creates a new Transaction that can be used to either send data to a
     * remote NiFi instance or receive data from a remote NiFi instance,
     * depending on the value passed for the {@code direction} argument.
     * </p>
     *
     * <p>
     * <b>Note:</b> If all of the nodes are penalized (See
     * {@link Builder#nodePenalizationPeriod(long, TimeUnit)}), then this method
     * will return <code>null</code>.
     * </p>
     *
     * @param direction specifies which direction the data should be
     * transferred. A value of {@link TransferDirection#SEND} indicates that
     * this Transaction will send data to the remote instance; a value of
     * {@link TransferDirection#RECEIVE} indicates that this Transaction will be
     * used to receive data from the remote instance.
     *
     * @return a Transaction to use for sending or receiving data, or
     * <code>null</code> if all nodes are penalized.
     * @throws org.apache.nifi.remote.exception.HandshakeException he
     * @throws org.apache.nifi.remote.exception.PortNotRunningException pnre
     * @throws IOException ioe
     * @throws org.apache.nifi.remote.exception.UnknownPortException upe
     */
    Transaction createTransaction(TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException;

    /**
     * <p>
     * In order to determine whether the server is configured for secure
     * communications, the client may have to query the server's RESTful
     * interface. Doing so could result in an IOException.
     * </p>
     *
     * @return {@code true} if site-to-site communications with the remote
     * instance are secure, {@code false} if site-to-site communications with
     * the remote instance are not secure. Whether or not communications are
     * secure depends on the server, not the client
     * @throws IOException if unable to query the remote instance's RESTful
     * interface or if the remote instance is not configured to allow
     * site-to-site communications
     */
    boolean isSecure() throws IOException;

    /**
     *
     * @return the configuration object that was built by the Builder
     */
    SiteToSiteClientConfig getConfig();

    /**
     * <p>
     * The Builder is the mechanism by which all configuration is passed to the
     * SiteToSiteClient. Once constructed, the SiteToSiteClient cannot be
     * reconfigured (i.e., it is immutable). If a change in configuration should
     * be desired, the client should be {@link Closeable#close() closed} and a
     * new client created.
     * </p>
     */
    class Builder implements Serializable {

        private static final long serialVersionUID = -4954962284343090219L;

        private Set<String> urls;
        private long timeoutNanos = TimeUnit.SECONDS.toNanos(30);
        private long penalizationNanos = TimeUnit.SECONDS.toNanos(3);
        private long idleExpirationNanos = TimeUnit.SECONDS.toNanos(30L);
        private long contentsCacheExpirationMillis = TimeUnit.SECONDS.toMillis(30L);
        private SSLContext sslContext;
        private String keystoreFilename;
        private String keystorePass;
        private KeystoreType keystoreType;
        private String truststoreFilename;
        private String truststorePass;
        private KeystoreType truststoreType;
        private EventReporter eventReporter = EventReporter.NO_OP;
        private File peerPersistenceFile;
        private boolean useCompression;
        private String portName;
        private String portIdentifier;
        private int batchCount;
        private long batchSize;
        private long batchNanos;
        private InetAddress localAddress;
        private SiteToSiteTransportProtocol transportProtocol = SiteToSiteTransportProtocol.RAW;
        private HttpProxy httpProxy;

        /**
         * Populates the builder with values from the provided config
         *
         * @param config to start with
         * @return the builder
         */
        public Builder fromConfig(final SiteToSiteClientConfig config) {
            this.urls = config.getUrls();
            this.timeoutNanos = config.getTimeout(TimeUnit.NANOSECONDS);
            this.penalizationNanos = config.getPenalizationPeriod(TimeUnit.NANOSECONDS);
            this.idleExpirationNanos = config.getIdleConnectionExpiration(TimeUnit.NANOSECONDS);
            this.contentsCacheExpirationMillis = config.getCacheExpiration(TimeUnit.MILLISECONDS);
            this.sslContext = config.getSslContext();
            this.keystoreFilename = config.getKeystoreFilename();
            this.keystorePass = config.getKeystorePassword();
            this.keystoreType = config.getKeystoreType();
            this.truststoreFilename = config.getTruststoreFilename();
            this.truststorePass = config.getTruststorePassword();
            this.truststoreType = config.getTruststoreType();
            this.eventReporter = config.getEventReporter();
            this.peerPersistenceFile = config.getPeerPersistenceFile();
            this.useCompression = config.isUseCompression();
            this.transportProtocol = config.getTransportProtocol();
            this.portName = config.getPortName();
            this.portIdentifier = config.getPortIdentifier();
            this.batchCount = config.getPreferredBatchCount();
            this.batchSize = config.getPreferredBatchSize();
            this.batchNanos = config.getPreferredBatchDuration(TimeUnit.NANOSECONDS);
            this.localAddress = config.getLocalAddress();
            this.httpProxy = config.getHttpProxy();

            return this;
        }

        /**
         * <p>Specifies the URL of the remote NiFi instance.</p>
         * <p>If this URL points to a NiFi node in a NiFi cluster, data transfer to and from
         * nodes will be automatically load balanced across the different nodes.</p>
         *
         * <p>For better connectivity with a NiFi cluster, use {@link #urls(Set)} instead.</p>
         *
         * @param url url of remote instance
         * @return the builder
         */
        public Builder url(final String url) {
            final Set<String> urls = new LinkedHashSet<>();
            if (url != null && url.length() > 0) {
                urls.add(url);
            }
            this.urls = urls;
            return this;
        }

        /**
         * <p>
         * Specifies the local address to use when communicating with the remote NiFi instance.
         * </p>
         *
         * @param localAddress the local address to use, or <code>null</code> to use <code>anyLocal</code> address.
         * @return the builder
         */
        public Builder localAddress(final InetAddress localAddress) {
            this.localAddress = localAddress;
            return this;
        }

        /**
         * <p>
         * Specifies the URLs of the remote NiFi instance.
         * </p>
         * <p>
         * If this URL points to a NiFi node in a NiFi cluster, data transfer to and from
         * nodes will be automatically load balanced across the different nodes.
         * </p>
         *
         * <p>
         * Multiple urls provide better connectivity with a NiFi cluster, able to connect
         * to the target cluster at long as one of the specified urls is accessible.
         * </p>
         *
         * @param urls urls of remote instance
         * @return the builder
         */
        public Builder urls(final Set<String> urls) {
            this.urls = urls;
            return this;
        }

        /**
         * Specifies the communications timeouts to use when interacting with
         * the remote instances. The default value is 30 seconds.
         *
         * @param timeout to use when interacting with remote instances
         * @param unit unit of time over which to interpret the given timeout
         * @return the builder
         */
        public Builder timeout(final long timeout, final TimeUnit unit) {
            this.timeoutNanos = unit.toNanos(timeout);
            return this;
        }

        /**
         * Specifies how long the contents of a remote NiFi instance should be cached before making
         * another web request to the remote instance.
         *
         * @param expirationPeriod the amount of time that an entry in the cache should expire
         * @param unit unit of time over which to interpret the given expirationPeriod
         * @return the builder
         */
        public Builder cacheExpiration(final long expirationPeriod, final TimeUnit unit) {
            this.contentsCacheExpirationMillis = unit.toMillis(expirationPeriod);
            return this;
        }

        /**
         * Specifies the amount of time that a connection can remain idle in the
         * connection pool before it is "expired" and shutdown. The default
         * value is 30 seconds.
         *
         * @param timeout to use when interacting with remote instances
         * @param unit unit of time over which to interpret the given timeout
         * @return the builder
         */
        public Builder idleExpiration(final long timeout, final TimeUnit unit) {
            this.idleExpirationNanos = unit.toNanos(timeout);
            return this;
        }

        /**
         * If there is a problem communicating with a node (i.e., any node in
         * the remote NiFi cluster or the remote instance of NiFi if it is
         * standalone), specifies how long the client should wait before
         * attempting to communicate with that node again. While a particular
         * node is penalized, all other nodes in the remote cluster (if any)
         * will still be available for communication. The default value is 3
         * seconds.
         *
         * @param period time to wait between communication attempts
         * @param unit over which to evaluate the given period
         * @return the builder
         */
        public Builder nodePenalizationPeriod(final long period, final TimeUnit unit) {
            this.penalizationNanos = unit.toNanos(period);
            return this;
        }

        /**
         * Specifies the SSL Context to use when communicating with the remote
         * NiFi instance(s). If not specified, communications will not be
         * secure. The remote instance of NiFi always determines whether or not
         * Site-to-Site communications are secure (i.e., the client will always
         * use secure or non-secure communications, depending on what the server
         * dictates). <b>Note:</b> The SSLContext provided by this method will be
         * ignored if using a Serializable Configuration (see {@link #buildSerializableConfig()}).
         * If a Serializable Configuration is required and communications are to be
         * secure, the {@link #keystoreFilename(String)}, {@link #keystorePass(String)},
         * {@link #keystoreType}, {@link #truststoreFilename}, {@link #truststorePass(String)},
         * and {@link #truststoreType(KeystoreType)} methods must be used instead.
         *
         * @param sslContext the context
         * @return the builder
         */
        public Builder sslContext(final SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * @return the filename to use for the Keystore in order to communicate securely
         * with the remote instance of NiFi
         */
        public String getKeystoreFilename() {
            return keystoreFilename;
        }

        /**
         * Sets the filename to use for the Keystore in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param keystoreFilename the filename to use for the Keystore in order to communicate securely
         *            with the remote instance of NiFi
         * @return the builder
         */
        public Builder keystoreFilename(final String keystoreFilename) {
            this.keystoreFilename = keystoreFilename;
            return this;
        }

        /**
         * @return the password to use for the Keystore in order to communicate securely
         * with the remote instance of NiFi
         */
        public String getKeystorePass() {
            return keystorePass;
        }

        /**
         * Sets the password to use for the Keystore in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param keystorePass the password to use for the Keystore in order to communicate securely
         *            with the remote instance of NiFi
         * @return the builder
         */
        public Builder keystorePass(final String keystorePass) {
            this.keystorePass = keystorePass;
            return this;
        }

        /**
         * @return the type of Keystore to use in order to communicate securely
         * with the remote instance of NiFi
         */
        public KeystoreType getKeystoreType() {
            return keystoreType;
        }

        /**
         * Sets the type of the Keystore to use in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param keystoreType the type of the Keystore to use in order to communicate securely
         *            with the remote instance of NiFi
         * @return the builder
         */
        public Builder keystoreType(final KeystoreType keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }

        /**
         * @return the filename to use for the Truststore in order to communicate securely
         * with the remote instance of NiFi
         */
        public String getTruststoreFilename() {
            return truststoreFilename;
        }

        /**
         * Sets the filename to use for the Truststore in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param truststoreFilename the filename to use for the Truststore in order to communicate securely
         *            with the remote instance of NiFi
         * @return the builder
         */
        public Builder truststoreFilename(final String truststoreFilename) {
            this.truststoreFilename = truststoreFilename;
            return this;
        }

        /**
         * @return the password to use for the Truststore in order to communicate securely
         * with the remote instance of NiFi
         */
        public String getTruststorePass() {
            return truststorePass;
        }

        /**
         * Sets the password to use for the Truststore in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param truststorePass the filename to use for the Truststore in order to communicate securely
         * with the remote instance of NiFi
         */
        public Builder truststorePass(final String truststorePass) {
            this.truststorePass = truststorePass;
            return this;
        }

        /**
         * @return the type of the Truststore to use in order to communicate securely
         * with the remote instance of NiFi
         */
        public KeystoreType getTruststoreType() {
            return truststoreType;
        }

        /**
         * Sets the password type of the Truststore to use in order to communicate securely
         * with the remote instance of NiFi
         *
         * @param truststoreType the type of the Truststore to use in order to communicate securely
         *            with the remote instance of NiFi
         * @return the builder
         */
        public Builder truststoreType(final KeystoreType truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        /**
         * Provides an EventReporter that can be used by the client in order to
         * report any events that could be of interest when communicating with
         * the remote instance. The EventReporter provided must be threadsafe.
         *
         * @param eventReporter reporter
         * @return the builder
         */
        public Builder eventReporter(final EventReporter eventReporter) {
            this.eventReporter = eventReporter;
            return this;
        }

        /**
         * Specifies a file that the client can write to in order to persist the
         * list of nodes in the remote cluster and recover the list of nodes
         * upon restart. This allows the client to function if the remote
         * Cluster Manager is unavailable, even after a restart of the client
         * software. If not specified, the list of nodes will not be persisted
         * and a failure of the Cluster Manager will result in not being able to
         * communicate with the remote instance if a new client is created.
         *
         * @param peerPersistenceFile file
         * @return the builder
         */
        public Builder peerPersistenceFile(final File peerPersistenceFile) {
            this.peerPersistenceFile = peerPersistenceFile;
            return this;
        }

        /**
         * Specifies whether or not data should be compressed before being
         * transferred to or from the remote instance.
         *
         * @param compress true if should compress
         * @return the builder
         */
        public Builder useCompression(final boolean compress) {
            this.useCompression = compress;
            return this;
        }

        /**
         * Specifies the protocol to use for site to site data transport.
         * @param transportProtocol transport protocol
         * @return the builder
         */
        public Builder transportProtocol(final SiteToSiteTransportProtocol transportProtocol) {
            this.transportProtocol = transportProtocol;
            return this;
        }

        /**
         * Specifies the name of the port to communicate with. Either the port
         * name or the port identifier must be specified.
         *
         * @param portName name of port
         * @return the builder
         */
        public Builder portName(final String portName) {
            this.portName = portName;
            return this;
        }

        /**
         * Specifies the unique identifier of the port to communicate with. If
         * it is known, this is preferred over providing the port name, as the
         * port name may change.
         *
         * @param portIdentifier identifier of port
         * @return the builder
         */
        public Builder portIdentifier(final String portIdentifier) {
            this.portIdentifier = portIdentifier;
            return this;
        }

        /**
         * When pulling data from a NiFi instance, the sender chooses how large
         * a Transaction is. However, the client has the ability to request a
         * particular batch size/duration. This method specifies the preferred
         * number of {@link DataPacket}s to include in a Transaction.
         *
         * @param count client preferred batch size
         * @return the builder
         */
        public Builder requestBatchCount(final int count) {
            this.batchCount = count;
            return this;
        }

        /**
         * When pulling data from a NiFi instance, the sender chooses how large
         * a Transaction is. However, the client has the ability to request a
         * particular batch size/duration. This method specifies the preferred
         * number of bytes to include in a Transaction.
         *
         * @param bytes client preferred batch size
         * @return the builder
         */
        public Builder requestBatchSize(final long bytes) {
            this.batchSize = bytes;
            return this;
        }

        /**
         * When pulling data from a NiFi instance, the sender chooses how large
         * a Transaction is. However, the client has the ability to request a
         * particular batch size/duration. This method specifies the preferred
         * amount of time that a Transaction should span.
         *
         * @param value client preferred batch duration
         * @param unit client preferred batch duration unit
         * @return the builder
         */
        public Builder requestBatchDuration(final long value, final TimeUnit unit) {
            this.batchNanos = unit.toNanos(value);
            return this;
        }

        /**
         * @return a {@link SiteToSiteClientConfig} for the configured values
         * but does not create a SiteToSiteClient
         */
        public SiteToSiteClientConfig buildConfig() {
            return new StandardSiteToSiteClientConfig(this);
        }

        /**
         * @return a new SiteToSiteClient that can be used to send and receive
         *         data with remote instances of NiFi
         *
         * @throws IllegalStateException if either the url is not set or neither
         *             the port name nor port identifier is set,
         *             or if the transport protocol is not supported.
         */
        public SiteToSiteClient build() {
            if (urls == null) {
                throw new IllegalStateException("Must specify URL to build Site-to-Site client");
            }

            if (portName == null && portIdentifier == null) {
                throw new IllegalStateException("Must specify either Port Name or Port Identifier to build Site-to-Site client");
            }

            switch (transportProtocol){
                case RAW:
                    return new SocketClient(buildConfig());
                case HTTP:
                    return new HttpClient(buildConfig());
                default:
                    throw new IllegalStateException("Transport protocol '" + transportProtocol + "' is not supported.");
            }
        }

        /**
         * @return the configured URL for the remote NiFi instance
         */
        public String getUrl() {
            if (urls != null && urls.size() > 0) {
                return urls.iterator().next();
            }
            return null;
        }

        /**
         * @param timeUnit unit over which to interpret the timeout
         * @return the communications timeout
         */
        public long getTimeout(final TimeUnit timeUnit) {
            return timeUnit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
        }

        /**
         * @param timeUnit unit over which to interpret the time
         * @return the amount of of time that a connection can remain idle in
         * the connection pool before being shutdown
         */
        public long getIdleConnectionExpiration(final TimeUnit timeUnit) {
            return timeUnit.convert(idleExpirationNanos, TimeUnit.NANOSECONDS);
        }

        /**
         * @param timeUnit unit of reported time
         * @return the amount of time that a particular node will be ignored
         * after a communications error with that node occurs
         */
        public long getPenalizationPeriod(TimeUnit timeUnit) {
            return timeUnit.convert(penalizationNanos, TimeUnit.NANOSECONDS);
        }

        /**
         * @return the SSL Context that is configured for this builder
         */
        public SSLContext getSslContext() {
            return sslContext;
        }

        /**
         * @return the EventReporter that is to be used by clients to report
         * events
         */
        public EventReporter getEventReporter() {
            return eventReporter;
        }

        /**
         * @return the file that is to be used for persisting the nodes of a
         * remote cluster, if any
         */
        public File getPeerPersistenceFile() {
            return peerPersistenceFile;
        }

        /**
         * @return a boolean indicating whether or not compression will be used
         * to transfer data to and from the remote instance
         */
        public boolean isUseCompression() {
            return useCompression;
        }

        /**
         * @return the transport protocol to use, defaults to RAW
         */
        public SiteToSiteTransportProtocol getTransportProtocol(){
            return transportProtocol;
        }

        /**
         * @return the name of the port that the client is to communicate with
         */
        public String getPortName() {
            return portName;
        }

        /**
         * @return the identifier of the port that the client is to communicate
         * with
         */
        public String getPortIdentifier() {
            return portIdentifier;
        }


        /**
         * Specify a HTTP proxy information to use with HTTP protocol of Site-to-Site communication.
         * @param httpProxy HTTP proxy information
         * @return the builder
         */
        public Builder httpProxy(final HttpProxy httpProxy) {
            this.httpProxy = httpProxy;
            return this;
        }

        public HttpProxy getHttpProxy() {
            return httpProxy;
        }

    }


    @SuppressWarnings("deprecation")
    class StandardSiteToSiteClientConfig implements SiteToSiteClientConfig, Serializable {

        private static final long serialVersionUID = 1L;

        // This Set instance has to be initialized here to be serialized via Kryo.
        private final Set<String> urls = new LinkedHashSet<>();
        private final long timeoutNanos;
        private final long penalizationNanos;
        private final long idleExpirationNanos;
        private final long contentsCacheExpirationMillis;
        private final SSLContext sslContext;
        private final String keystoreFilename;
        private final String keystorePass;
        private final KeystoreType keystoreType;
        private final String truststoreFilename;
        private final String truststorePass;
        private final KeystoreType truststoreType;
        private final EventReporter eventReporter;
        private final File peerPersistenceFile;
        private final boolean useCompression;
        private final SiteToSiteTransportProtocol transportProtocol;
        private final String portName;
        private final String portIdentifier;
        private final int batchCount;
        private final long batchSize;
        private final long batchNanos;
        private final HttpProxy httpProxy;
        private final InetAddress localAddress;

        // some serialization frameworks require a default constructor
        private StandardSiteToSiteClientConfig() {
            this.timeoutNanos = 0;
            this.penalizationNanos = 0;
            this.idleExpirationNanos = 0;
            this.contentsCacheExpirationMillis = 30000L;
            this.sslContext = null;
            this.keystoreFilename = null;
            this.keystorePass = null;
            this.keystoreType = null;
            this.truststoreFilename = null;
            this.truststorePass = null;
            this.truststoreType = null;
            this.eventReporter = null;
            this.peerPersistenceFile = null;
            this.useCompression = false;
            this.portName = null;
            this.portIdentifier = null;
            this.batchCount = 0;
            this.batchSize = 0;
            this.batchNanos = 0;
            this.transportProtocol = null;
            this.httpProxy = null;
            this.localAddress = null;
        }

        private StandardSiteToSiteClientConfig(final SiteToSiteClient.Builder builder) {
            if (builder.urls != null) {
                this.urls.addAll(builder.urls);
            }
            this.timeoutNanos = builder.timeoutNanos;
            this.penalizationNanos = builder.penalizationNanos;
            this.idleExpirationNanos = builder.idleExpirationNanos;
            this.contentsCacheExpirationMillis = builder.contentsCacheExpirationMillis;
            this.sslContext = builder.sslContext;
            this.keystoreFilename = builder.keystoreFilename;
            this.keystorePass = builder.keystorePass;
            this.keystoreType = builder.keystoreType;
            this.truststoreFilename = builder.truststoreFilename;
            this.truststorePass = builder.truststorePass;
            this.truststoreType = builder.truststoreType;
            this.eventReporter = builder.eventReporter;
            this.peerPersistenceFile = builder.peerPersistenceFile;
            this.useCompression = builder.useCompression;
            this.portName = builder.portName;
            this.portIdentifier = builder.portIdentifier;
            this.batchCount = builder.batchCount;
            this.batchSize = builder.batchSize;
            this.batchNanos = builder.batchNanos;
            this.transportProtocol = builder.getTransportProtocol();
            this.httpProxy = builder.getHttpProxy();
            this.localAddress = builder.localAddress;
        }

        @Override
        public boolean isUseCompression() {
            return useCompression;
        }

        @Override
        public String getUrl() {
            if (urls != null && urls.size() > 0) {
                return urls.iterator().next();
            }
            return null;
        }

        @Override
        public Set<String> getUrls() {
            return urls;
        }

        @Override
        public long getTimeout(final TimeUnit timeUnit) {
            return timeUnit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public long getCacheExpiration(final TimeUnit timeUnit) {
            return timeUnit.convert(contentsCacheExpirationMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public long getIdleConnectionExpiration(final TimeUnit timeUnit) {
            return timeUnit.convert(idleExpirationNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public SSLContext getSslContext() {
            if (sslContext != null) {
                return sslContext;
            }

            final KeyManagerFactory keyManagerFactory;
            if (keystoreFilename != null && keystorePass != null && keystoreType != null) {
                try {
                    // prepare the keystore
                    final KeyStore keyStore = KeyStoreUtils.getKeyStore(getKeystoreType().name());
                    try (final InputStream keyStoreStream = new FileInputStream(new File(getKeystoreFilename()))) {
                        keyStore.load(keyStoreStream, keystorePass.toCharArray());
                    }
                    keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    keyManagerFactory.init(keyStore, keystorePass.toCharArray());
                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to load Keystore", e);
                }
            } else {
                keyManagerFactory = null;
            }

            final TrustManagerFactory trustManagerFactory;
            if (truststoreFilename != null && truststorePass != null && truststoreType != null) {
                try {
                    // prepare the truststore
                    final KeyStore trustStore = KeyStoreUtils.getTrustStore(getTruststoreType().name());
                    try (final InputStream trustStoreStream = new FileInputStream(new File(getTruststoreFilename()))) {
                        trustStore.load(trustStoreStream, truststorePass.toCharArray());
                    }
                    trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    trustManagerFactory.init(trustStore);
                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to load Truststore", e);
                }
            } else {
                trustManagerFactory = null;
            }

            if (keyManagerFactory != null && trustManagerFactory != null) {
                try {
                    // initialize the ssl context
                    final SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
                    sslContext.getDefaultSSLParameters().setNeedClientAuth(true);

                    return sslContext;
                } catch (final Exception e) {
                    throw new IllegalStateException("Created keystore and truststore but failed to initialize SSLContext", e);
                }
            } else {
                return null;
            }
        }

        @Override
        public String getPortName() {
            return portName;
        }

        @Override
        public String getPortIdentifier() {
            return portIdentifier;
        }

        @Override
        public long getPenalizationPeriod(final TimeUnit timeUnit) {
            return timeUnit.convert(penalizationNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public File getPeerPersistenceFile() {
            return peerPersistenceFile;
        }

        @Override
        public EventReporter getEventReporter() {
            return eventReporter;
        }

        @Override
        public long getPreferredBatchDuration(final TimeUnit timeUnit) {
            return timeUnit.convert(batchNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public long getPreferredBatchSize() {
            return batchSize;
        }

        @Override
        public int getPreferredBatchCount() {
            return batchCount;
        }

        @Override
        public String getKeystoreFilename() {
            return keystoreFilename;
        }

        @Override
        public String getKeystorePassword() {
            return keystorePass;
        }

        @Override
        public KeystoreType getKeystoreType() {
            return keystoreType;
        }

        @Override
        public String getTruststoreFilename() {
            return truststoreFilename;
        }

        @Override
        public String getTruststorePassword() {
            return truststorePass;
        }

        @Override
        public KeystoreType getTruststoreType() {
            return truststoreType;
        }

        @Override
        public SiteToSiteTransportProtocol getTransportProtocol() {
            return transportProtocol;
        }

        @Override
        public HttpProxy getHttpProxy() {
            return httpProxy;
        }

        @Override
        public InetAddress getLocalAddress() {
            return localAddress;
        }
    }
}
