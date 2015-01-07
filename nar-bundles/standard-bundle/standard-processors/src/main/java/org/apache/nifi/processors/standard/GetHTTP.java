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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnShutdown;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.util.StopWatch;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

@Tags({"get", "fetch", "poll", "http", "https", "ingest", "source", "input"})
@CapabilityDescription("Fetches a file via HTTP")
public class GetHTTP extends AbstractSessionFactoryProcessor {

    static final int PERSISTENCE_INTERVAL_MSEC = 10000;

    public static final String HEADER_IF_NONE_MATCH = "If-None-Match";
    public static final String HEADER_IF_MODIFIED_SINCE = "If-Modified-Since";
    public static final String HEADER_ACCEPT = "Accept";
    public static final String HEADER_LAST_MODIFIED = "Last-Modified";
    public static final String HEADER_ETAG = "ETag";
    public static final int NOT_MODIFIED = 304;

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("The URL to pull from")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("https?\\://.*")))
            .build();
    public static final PropertyDescriptor FOLLOW_REDIRECTS = new PropertyDescriptor.Builder()
            .name("Follow Redirects")
            .description(
                    "If we receive a 3xx HTTP Status Code from the server, indicates whether or not we should follow the redirect that the server specifies")
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("How long to wait when attempting to connect to the remote server before giving up")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor ACCEPT_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Accept Content-Type")
            .description("If specified, requests will only accept the provided Content-Type")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Data Timeout")
            .description(
                    "How long to wait between receiving segments of data from the remote server before giving up and discarding the partial file")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .description("The filename to assign to the file when pulled")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username required to access the URL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password required to access the URL")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USER_AGENT = new PropertyDescriptor.Builder()
            .name("User Agent")
            .description("What to report as the User Agent when we connect to the remote server")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All files are transferred to the success relationship").build();

    public static final String LAST_MODIFIED_DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

    // package access to enable unit testing
    static final String UNINITIALIZED_LAST_MODIFIED_VALUE;

    private static final String HTTP_CACHE_FILE_PREFIX = "conf/.httpCache-";

    static final String ETAG = "ETag";

    static final String LAST_MODIFIED = "LastModified";

    static {
        SimpleDateFormat sdf = new SimpleDateFormat(LAST_MODIFIED_DATE_PATTERN_RFC1123, Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        UNINITIALIZED_LAST_MODIFIED_VALUE = sdf.format(new Date(1L));
    }
    final AtomicReference<String> lastModifiedRef = new AtomicReference<>(UNINITIALIZED_LAST_MODIFIED_VALUE);
    final AtomicReference<String> entityTagRef = new AtomicReference<>("");
    // end

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private volatile long timeToPersist = 0;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock readLock = lock.readLock();
    private final WriteLock writeLock = lock.writeLock();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(URL);
        properties.add(FILENAME);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(CONNECTION_TIMEOUT);
        properties.add(DATA_TIMEOUT);
        properties.add(USER_AGENT);
        properties.add(ACCEPT_CONTENT_TYPE);
        properties.add(FOLLOW_REDIRECTS);
        this.properties = Collections.unmodifiableList(properties);

        // load etag and lastModified from file
        File httpCache = new File(HTTP_CACHE_FILE_PREFIX + getIdentifier());
        try (FileInputStream fis = new FileInputStream(httpCache)) {
            Properties props = new Properties();
            props.load(fis);
            entityTagRef.set(props.getProperty(ETAG));
            lastModifiedRef.set(props.getProperty(LAST_MODIFIED));
        } catch (IOException swallow) {
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        entityTagRef.set("");
        lastModifiedRef.set(UNINITIALIZED_LAST_MODIFIED_VALUE);
    }

    @OnShutdown
    public void onShutdown() {
        File httpCache = new File(HTTP_CACHE_FILE_PREFIX + getIdentifier());
        try (FileOutputStream fos = new FileOutputStream(httpCache)) {
            Properties props = new Properties();
            props.setProperty(ETAG, entityTagRef.get());
            props.setProperty(LAST_MODIFIED, lastModifiedRef.get());
            props.store(fos, "GetHTTP file modification values");
        } catch (IOException swallow) {
        }

    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        if (context.getProperty(URL).getValue().startsWith("https") && context.getProperty(SSL_CONTEXT_SERVICE).getValue() == null) {
            results.add(new ValidationResult.Builder()
                    .explanation("URL is set to HTTPS protocol but no SSLContext has been specified")
                    .valid(false)
                    .subject("SSL Context")
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessorLog logger = getLogger();

        final ProcessSession session = sessionFactory.createSession();
        final FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile != null) {
            session.transfer(incomingFlowFile, REL_SUCCESS);
            logger.warn("found FlowFile {} in input queue; transferring to success", new Object[]{incomingFlowFile});
        }

        final String url = context.getProperty(URL).getValue();
        final URI uri;
        String source = url;
        try {
            uri = new URI(url);
            source = uri.getHost();
        } catch (URISyntaxException swallow) {
            // this won't happen as the url has already been validated
        }
        final ClientConnectionManager conMan = createConnectionManager(context);
        try {
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)
                    .intValue());
            HttpConnectionParams.setSoTimeout(httpParams, context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            httpParams.setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, context.getProperty(FOLLOW_REDIRECTS).asBoolean());
            final String userAgent = context.getProperty(USER_AGENT).getValue();
            if (userAgent != null) {
                httpParams.setParameter("http.useragent", userAgent);
            }

            final HttpClient client = new DefaultHttpClient(conMan, httpParams);

            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();
            if (username != null) {
                if (password == null) {
                    ((DefaultHttpClient) client).getCredentialsProvider().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username));
                } else {
                    ((DefaultHttpClient) client).getCredentialsProvider().setCredentials(AuthScope.ANY,
                            new UsernamePasswordCredentials(username, password));
                }
            }

            final HttpGet get = new HttpGet(url);

            get.addHeader(HEADER_IF_MODIFIED_SINCE, lastModifiedRef.get());
            get.addHeader(HEADER_IF_NONE_MATCH, entityTagRef.get());

            final String accept = context.getProperty(ACCEPT_CONTENT_TYPE).getValue();
            if (accept != null) {
                get.addHeader(HEADER_ACCEPT, accept);
            }

            try {
                final StopWatch stopWatch = new StopWatch(true);
                final HttpResponse response = client.execute(get);
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == NOT_MODIFIED) {
                    logger.info("content not retrieved because server returned HTTP Status Code {}: Not Modified", new Object[]{NOT_MODIFIED});
                    context.yield();
                    // doing a commit in case there were flow files in the input queue
                    session.commit();
                    return;
                }
                final String statusExplanation = response.getStatusLine().getReasonPhrase();

                if (statusCode >= 300) {
                    logger.error("received status code {}:{} from {}", new Object[]{statusCode, statusExplanation, url});
                    // doing a commit in case there were flow files in the input queue
                    session.commit();
                    return;
                }

                FlowFile flowFile = session.create();
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), context.getProperty(FILENAME).getValue());
                flowFile = session.putAttribute(flowFile, this.getClass().getSimpleName().toLowerCase() + ".remote.source", source);
                flowFile = session.importFrom(response.getEntity().getContent(), flowFile);
                final long flowFileSize = flowFile.getSize();
                stopWatch.stop();
                final String dataRate = stopWatch.calculateDataRate(flowFileSize);
                session.getProvenanceReporter().receive(flowFile, url, stopWatch.getDuration(TimeUnit.MILLISECONDS));
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("Successfully received {} from {} at a rate of {}; transferred to success", new Object[]{flowFile, url, dataRate});
                session.commit();
                final Header lastModified = response.getFirstHeader(HEADER_LAST_MODIFIED);
                if (lastModified != null) {
                    lastModifiedRef.set(lastModified.getValue());
                }

                final Header etag = response.getFirstHeader(HEADER_ETAG);
                if (etag != null) {
                    entityTagRef.set(etag.getValue());
                }
                if ((etag != null || lastModified != null) && readLock.tryLock()) {
                    try {
                        if (timeToPersist < System.currentTimeMillis()) {
                            readLock.unlock();
                            writeLock.lock();
                            if (timeToPersist < System.currentTimeMillis()) {
                                try {
                                    timeToPersist = System.currentTimeMillis() + PERSISTENCE_INTERVAL_MSEC;
                                    File httpCache = new File(HTTP_CACHE_FILE_PREFIX + getIdentifier());
                                    try (FileOutputStream fos = new FileOutputStream(httpCache)) {
                                        Properties props = new Properties();
                                        props.setProperty(ETAG, entityTagRef.get());
                                        props.setProperty(LAST_MODIFIED, lastModifiedRef.get());
                                        props.store(fos, "GetHTTP file modification values");
                                    } catch (IOException e) {
                                        getLogger().error("Failed to persist ETag and LastMod due to " + e, e);
                                    }
                                } finally {
                                    readLock.lock();
                                    writeLock.unlock();
                                }
                            }
                        }
                    } finally {
                        readLock.unlock();
                    }
                }
            } catch (final IOException e) {
                context.yield();
                session.rollback();
                logger.error("Failed to retrieve file from {} due to {}; rolling back session", new Object[]{url, e.getMessage()}, e);
                throw new ProcessException(e);
            } catch (final Throwable t) {
                context.yield();
                session.rollback();
                logger.error("Failed to process due to {}; rolling back session", new Object[]{t.getMessage()}, t);
                throw t;
            }

        } finally {
            conMan.shutdown();
        }
    }

    private ClientConnectionManager createConnectionManager(final ProcessContext processContext) {
        final String url = processContext.getProperty(URL).getValue();
        final boolean secure = (url.toLowerCase().startsWith("https"));
        URI uriObject;
        try {
            uriObject = new URI(url);
        } catch (URISyntaxException e) {
            throw new ProcessException(e); // will not happen because of our validators
        }
        int port = uriObject.getPort();
        if (port == -1) {
            port = 443;
        }

        final ClientConnectionManager conMan = new BasicClientConnectionManager();
        if (secure) {
            try {
                final SSLContext context = createSslContext(processContext);
                final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(context);
                final Scheme sslScheme = new Scheme("https", port, sslSocketFactory);
                conMan.getSchemeRegistry().register(sslScheme);
            } catch (final Exception e) {
                getLogger().error("Unable to setup SSL connection due to ", e);
                return null;
            }
        }

        return conMan;
    }

    /**
     * Creates a SSL context based on the processor's optional properties.
     * <p/>
     *
     * @return a SSLContext instance
     * <p/>
     * @throws ProcessingException if the context could not be created
     */
    private SSLContext createSslContext(final ProcessContext context) {
        final SSLContextService service = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        return (service == null) ? null : service.createSSLContext(ClientAuth.REQUIRED);
    }
}
