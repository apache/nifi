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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.servlets.ContentAcknowledgmentServlet;
import org.apache.nifi.processors.standard.servlets.ListenHTTPServlet;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.LeakyBucketStreamThrottler;
import org.apache.nifi.stream.io.StreamThrottler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "http", "https", "rest", "listen"})
@CapabilityDescription("Starts an HTTP Server and listens on a given base path to transform incoming requests into FlowFiles. "
    + "The default URI of the Service will be http://{hostname}:{port}/contentListener. Only HEAD and POST requests are "
    + "supported. GET, PUT, and DELETE will result in an error and the HTTP response status code 405.")
public class ListenHTTP extends AbstractSessionFactoryProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder().name("success").description("Relationship for successfully received FlowFiles").build();

    public static final PropertyDescriptor BASE_PATH = new PropertyDescriptor.Builder().name("Base Path").description("Base path for incoming connections").required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("contentListener").addValidator(StandardValidators.URI_VALIDATOR)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("(^[^/]+.*[^/]+$|^[^/]+$|^$)"))) // no start with / or end with /
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder().name("Listening Port").description("The Port to listen on for incoming connections").required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor AUTHORIZED_DN_PATTERN = new PropertyDescriptor.Builder().name("Authorized DN Pattern")
        .description("A Regular Expression to apply against the Distinguished Name of incoming connections. If the Pattern does not match the DN, the connection will be refused.").required(true)
        .defaultValue(".*").addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();
    public static final PropertyDescriptor MAX_UNCONFIRMED_TIME = new PropertyDescriptor.Builder().name("Max Unconfirmed Flowfile Time")
        .description("The maximum amount of time to wait for a FlowFile to be confirmed before it is removed from the cache").required(true).defaultValue("60 secs")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();
    public static final PropertyDescriptor OLD_MAX_DATA_RATE = new PropertyDescriptor.Builder().name("Max Data to Receive per Second")
        .description("Deprecated: Use 'Max Data Rate', which is based on 5 minutes").required(false).dynamic(true).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).build();
    public static final PropertyDescriptor MAX_DATA_RATE = new PropertyDescriptor.Builder().name("max_data_rate").displayName("Max Data Rate (5 mins)")
        .description("The maximum amount of data to receive per 5 minutes; this allows the bandwidth to be throttled to a specified rate; if not "
            + "specified, the data rate is not throttled. Specify data unit; e.g. 5 KB, 10 MB, 2 GB")
        .required(false).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder().name("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context").required(false).identifiesControllerService(RestrictedSSLContextService.class).build();
    public static final PropertyDescriptor HEADERS_AS_ATTRIBUTES_REGEX = new PropertyDescriptor.Builder().name("HTTP Headers to receive as Attributes (Regex)")
        .description("Specifies the Regular Expression that determines the names of HTTP Headers that should be passed along as FlowFile attributes")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).required(false).build();
    public static final PropertyDescriptor RETURN_CODE = new PropertyDescriptor.Builder().name("Return Code").description("The HTTP return code returned after every HTTP call")
        .defaultValue(String.valueOf(HttpServletResponse.SC_OK)).addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MULTIPART_REQUEST_MAX_SIZE = new PropertyDescriptor.Builder().name("multipart-request-max-size").displayName("Multipart Request Max Size")
        .description("The max size of the request. Only applies for requests with Content-Type: multipart/form-data, "
            + "and is used to prevent denial of service type of attacks, to prevent filling up the heap or disk space")
        .required(true).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).defaultValue("1 MB").build();
    public static final PropertyDescriptor MULTIPART_READ_BUFFER_SIZE = new PropertyDescriptor.Builder().name("multipart-read-buffer-size").displayName("Multipart Read Buffer Size")
        .description("The threshold size, at which the contents of an incoming file would be written to disk. " + "Only applies for requests with Content-Type: multipart/form-data. "
            + "It is used to prevent denial of service type of attacks, to prevent filling up the heap or disk space.")
        .required(true).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).defaultValue("512 KB").build();
    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder().name("request-timeout").displayName("Request Time Out")
        .description("The maximum amount of time to wait for a buffer of data; 0 implies wait forever").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).defaultValue("30 secs").required(true)
        .build();

    public static final String CONTEXT_ATTRIBUTE_PROCESSOR = "processor";
    public static final String CONTEXT_ATTRIBUTE_LOGGER = "logger";
    public static final String CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER = "sessionFactoryHolder";
    public static final String CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER = "processContextHolder";
    public static final String CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN = "authorityPattern";
    public static final String CONTEXT_ATTRIBUTE_HEADER_PATTERN = "headerPattern";
    public static final String CONTEXT_ATTRIBUTE_FLOWFILE_MAP = "flowFileMap";
    public static final String CONTEXT_ATTRIBUTE_STREAM_THROTTLER = "streamThrottler";
    public static final String CONTEXT_ATTRIBUTE_BASE_PATH = "basePath";
    public static final String CONTEXT_ATTRIBUTE_RETURN_CODE = "returnCode";
    public static final String CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE = "multipartRequestMaxSize";
    public static final String CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE = "multipartReadBufferSize";

    private volatile Server server = null;
    private final ConcurrentMap<String, FlowFileEntryTimeWrapper> flowFileMap = new ConcurrentHashMap<>();
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private final AtomicReference<StreamThrottler> throttlerRef = new AtomicReference<>();
    private volatile String oldMaxDataPerSecond;
    private volatile long maxBytesPerSecond;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(BASE_PATH);
        descriptors.add(PORT);
        descriptors.add(MAX_DATA_RATE);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(AUTHORIZED_DN_PATTERN);
        descriptors.add(MAX_UNCONFIRMED_TIME);
        descriptors.add(HEADERS_AS_ATTRIBUTES_REGEX);
        descriptors.add(RETURN_CODE);
        descriptors.add(MULTIPART_REQUEST_MAX_SIZE);
        descriptors.add(MULTIPART_READ_BUFFER_SIZE);
        descriptors.add(REQUEST_TIMEOUT);
        this.properties = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (propertyDescriptorName.equals(OLD_MAX_DATA_RATE.getName())) {
            return OLD_MAX_DATA_RATE;
        }
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        oldMaxDataPerSecond = null;
        for (Entry<PropertyDescriptor, String> prop : validationContext.getProperties().entrySet()) {
            PropertyDescriptor propDesc = prop.getKey();
            if (propDesc.isDynamic()) {
                oldMaxDataPerSecond = prop.getValue();
                getLogger().warn("'Max Data to Receive per Second' is deprecated...please remove it and use 'Max Data Rate'. It's equivalent " + "value would be 300x this value...5 mins vs 1 sec.");
            }
        }
        return Collections.emptySet();
    }

    @OnStopped
    public void shutdownHttpServer() {
        final StreamThrottler throttler = throttlerRef.getAndSet(null);
        if (throttler != null) {
            try {
                throttler.close();
            } catch (IOException e) {
                getLogger().error("Failed to close StreamThrottler", e);
            }
        }

        final Server toShutdown = this.server;
        if (toShutdown == null) {
            return;
        }

        shutdownHttpServer(toShutdown);
    }

    private void shutdownHttpServer(Server toShutdown) {
        try {
            toShutdown.stop();
            toShutdown.destroy();
        } catch (final Exception ex) {
            getLogger().warn("unable to cleanly shutdown embedded server due to {}", new Object[] {ex});
            this.server = null;
        }
    }

    private void createHttpServerFromService(final ProcessContext context) throws Exception {
        final String basePath = context.getProperty(BASE_PATH).evaluateAttributeExpressions().getValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final long requestTimeoutMS = context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final StreamThrottler streamThrottler = maxBytesPerSecond == 0 ? null : new LeakyBucketStreamThrottler(maxBytesPerSecond, requestTimeoutMS);
        final int returnCode = context.getProperty(RETURN_CODE).asInteger();
        long requestMaxSize = context.getProperty(MULTIPART_REQUEST_MAX_SIZE).asDataSize(DataUnit.B).longValue();
        int readBufferSize = context.getProperty(MULTIPART_READ_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        throttlerRef.set(streamThrottler);

        final boolean needClientAuth = sslContextService != null && sslContextService.getTrustStoreFile() != null;

        final SslContextFactory contextFactory = new SslContextFactory();
        contextFactory.setNeedClientAuth(needClientAuth);

        // Need to set SslContextFactory's endpointIdentificationAlgorithm to null; this is a server,
        // not a client. Server does not need to perform hostname verification on the client.
        // Previous to Jetty 9.4.15.v20190215, this defaulted to null, and now defaults to "HTTPS".
        contextFactory.setEndpointIdentificationAlgorithm(null);

        if (needClientAuth) {
            contextFactory.setTrustStorePath(sslContextService.getTrustStoreFile());
            contextFactory.setTrustStoreType(sslContextService.getTrustStoreType());
            contextFactory.setTrustStorePassword(sslContextService.getTrustStorePassword());
        }

        final String keystorePath = sslContextService == null ? null : sslContextService.getKeyStoreFile();
        if (keystorePath != null) {
            final String keystorePassword = sslContextService.getKeyStorePassword();
            final String keyStoreType = sslContextService.getKeyStoreType();

            contextFactory.setKeyStorePath(keystorePath);
            contextFactory.setKeyManagerPassword(keystorePassword);
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyStoreType(keyStoreType);
        }

        if (sslContextService != null) {
            contextFactory.setProtocol(sslContextService.getSslAlgorithm());
        }

        // thread pool for the jetty instance
        final QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setName(String.format("%s (%s) Web Server", getClass().getSimpleName(), getIdentifier()));

        // create the server instance
        final Server server = new Server(threadPool);

        // get the configured port
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        final ServerConnector connector;
        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        if (keystorePath == null) {
            // create the connector
            connector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
        } else {
            // configure the ssl connector
            httpConfiguration.setSecureScheme("https");
            httpConfiguration.setSecurePort(port);
            httpConfiguration.addCustomizer(new SecureRequestCustomizer());

            // build the connector

            connector = new ServerConnector(server, new SslConnectionFactory(contextFactory, "http/1.1"), new HttpConnectionFactory(httpConfiguration));
        }

        if (requestTimeoutMS != connector.getIdleTimeout()) {
            connector.setIdleTimeout(requestTimeoutMS);
        }

        // configure the port
        connector.setPort(port);

        // add the connector to the server
        server.setConnectors(new Connector[] {connector});

        final ServletContextHandler contextHandler = new ServletContextHandler(server, "/", true, (keystorePath != null));
        for (final Class<? extends Servlet> cls : getServerClasses()) {
            final Path path = cls.getAnnotation(Path.class);
            // Note: servlets must have a path annotation - this will NPE otherwise
            // also, servlets other than ListenHttpServlet must have a path starting with /
            if (basePath.isEmpty() && !path.value().isEmpty()) {
                // Note: this is to handle the condition of an empty uri, otherwise pathSpec would start with //
                contextHandler.addServlet(cls, path.value());
            } else {
                contextHandler.addServlet(cls, "/" + basePath + path.value());
            }
        }

        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_PROCESSOR, this);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_LOGGER, getLogger());
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER, sessionFactoryReference);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER, context);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_FLOWFILE_MAP, flowFileMap);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN, Pattern.compile(context.getProperty(AUTHORIZED_DN_PATTERN).getValue()));
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_STREAM_THROTTLER, streamThrottler);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_BASE_PATH, basePath);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_RETURN_CODE, returnCode);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE, requestMaxSize);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE, readBufferSize);

        if (context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).isSet()) {
            contextHandler.setAttribute(CONTEXT_ATTRIBUTE_HEADER_PATTERN, Pattern.compile(context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).getValue()));
        }
        try {
            server.start();
        } catch (Exception e) {
            shutdownHttpServer(server);
            throw e;
        }

        this.server = server;
    }

    @OnScheduled
    public void createHttpServer(final ProcessContext context) throws Exception {
        maxBytesPerSecond = 0;
        if (context.getProperty(MAX_DATA_RATE).isSet()) {
            maxBytesPerSecond = context.getProperty(MAX_DATA_RATE).asDataSize(DataUnit.B).longValue() / 300;
        } else if (oldMaxDataPerSecond != null) {
            getLogger().warn("'Max Data to Receive per Second' is deprecated, but will be used...remove it and use 'Max Data Rate', its "
                + "equivalent value is 300x this value due to the change in interval -- 5 min vs 1 sec.");
            maxBytesPerSecond = DataUnit.parseDataSize(oldMaxDataPerSecond, DataUnit.B).longValue();
        }
        createHttpServerFromService(context);
    }

    protected Set<Class<? extends Servlet>> getServerClasses() {
        final Set<Class<? extends Servlet>> s = new HashSet<>();
        // NOTE: Servlets added below MUST have a Path annotation
        // any servlets other than ListenHTTPServlet must have a Path annotation start with /
        s.add(ListenHTTPServlet.class);
        s.add(ContentAcknowledgmentServlet.class);
        return s;
    }

    private Set<String> findOldFlowFileIds(final ProcessContext ctx) {
        final Set<String> old = new HashSet<>();

        final long expiryMillis = ctx.getProperty(MAX_UNCONFIRMED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final long cutoffTime = System.currentTimeMillis() - expiryMillis;
        for (final Map.Entry<String, FlowFileEntryTimeWrapper> entry : flowFileMap.entrySet()) {
            final FlowFileEntryTimeWrapper wrapper = entry.getValue();
            if (wrapper != null && wrapper.getEntryTime() < cutoffTime) {
                old.add(entry.getKey());
            }
        }

        return old;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        sessionFactoryReference.compareAndSet(null, sessionFactory);

        for (final String id : findOldFlowFileIds(context)) {
            final FlowFileEntryTimeWrapper wrapper = flowFileMap.remove(id);
            if (wrapper != null) {
                getLogger().warn("failed to received acknowledgment for HOLD with ID {} sent by {}; rolling back session", new Object[] {id, wrapper.getClientIP()});
                wrapper.session.rollback();
            }
        }

        context.yield();
    }

    public static class FlowFileEntryTimeWrapper {

        private final Set<FlowFile> flowFiles;
        private final long entryTime;
        private final ProcessSession session;
        private final String clientIP;

        public FlowFileEntryTimeWrapper(final ProcessSession session, final Set<FlowFile> flowFiles, final long entryTime, final String clientIP) {
            this.flowFiles = flowFiles;
            this.entryTime = entryTime;
            this.session = session;
            this.clientIP = clientIP;
        }

        public Set<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public long getEntryTime() {
            return entryTime;
        }

        public ProcessSession getSession() {
            return session;
        }

        public String getClientIP() {
            return clientIP;
        }
    }
}
