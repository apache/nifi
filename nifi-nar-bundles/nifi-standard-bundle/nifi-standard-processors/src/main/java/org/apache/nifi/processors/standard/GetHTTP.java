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

import static org.apache.nifi.processors.standard.util.HTTPUtils.PROXY_HOST;
import static org.apache.nifi.processors.standard.util.HTTPUtils.PROXY_PORT;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

@Tags({"get", "fetch", "poll", "http", "https", "ingest", "source", "input"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Fetches data from an HTTP or HTTPS URL and writes the data to the content of a FlowFile. Once the content has been fetched, the ETag and Last Modified "
        + "dates are remembered (if the web server supports these concepts). This allows the Processor to fetch new data only if the remote data has changed or until the state is cleared. That is, "
        + "once the content has been fetched from the given URL, it will not be fetched again until the content on the remote server changes. Note that due to limitations on state "
        + "management, stored \"last modified\" and etag fields never expire. If the URL in GetHttp uses Expression Language that is unbounded, there "
        + "is the potential for Out of Memory Errors to occur.")
@DynamicProperties({
        @DynamicProperty(name = "Header Name", value = "The Expression Language to be used to populate the header value",
                expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
                description = "The additional headers to be sent by the processor whenever making a new HTTP request. \n " +
                        "Setting a dynamic property name to XYZ and value to ${attribute} will result in the header 'XYZ: attribute_value' being sent to the HTTP endpoint"),
})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the remote server"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type of the FlowFile, as reported by the HTTP Content-Type header")
})
@Stateful(scopes = {Scope.LOCAL}, description = "Stores Last Modified Time and ETag headers returned by server so that the same data will not be fetched multiple times.")
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
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("https?\\://.*")))
            .build();
    public static final PropertyDescriptor FOLLOW_REDIRECTS = new PropertyDescriptor.Builder()
            .name("Follow Redirects")
            .description("If we receive a 3xx HTTP Status Code from the server, indicates whether or not we should follow the redirect that the server specifies")
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
            .description("How long to wait between receiving segments of data from the remote server before giving up and discarding the partial file")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .description("The filename to assign to the file when pulled")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
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

    public static final String DEFAULT_COOKIE_POLICY_STR = "default";
    public static final String STANDARD_COOKIE_POLICY_STR = "standard";
    public static final String STRICT_COOKIE_POLICY_STR = "strict";
    public static final String NETSCAPE_COOKIE_POLICY_STR = "netscape";
    public static final String IGNORE_COOKIE_POLICY_STR = "ignore";
    public static final AllowableValue DEFAULT_COOKIE_POLICY = new AllowableValue(DEFAULT_COOKIE_POLICY_STR, DEFAULT_COOKIE_POLICY_STR,
            "Default cookie policy that provides a higher degree of compatibility with common cookie management of popular HTTP agents for non-standard (Netscape style) cookies.");
    public static final AllowableValue STANDARD_COOKIE_POLICY = new AllowableValue(STANDARD_COOKIE_POLICY_STR, STANDARD_COOKIE_POLICY_STR,
            "RFC 6265 compliant cookie policy (interoperability profile).");
    public static final AllowableValue STRICT_COOKIE_POLICY = new AllowableValue(STRICT_COOKIE_POLICY_STR, STRICT_COOKIE_POLICY_STR,
            "RFC 6265 compliant cookie policy (strict profile).");
    public static final AllowableValue NETSCAPE_COOKIE_POLICY = new AllowableValue(NETSCAPE_COOKIE_POLICY_STR, NETSCAPE_COOKIE_POLICY_STR,
            "Netscape draft compliant cookie policy.");
    public static final AllowableValue IGNORE_COOKIE_POLICY = new AllowableValue(IGNORE_COOKIE_POLICY_STR, IGNORE_COOKIE_POLICY_STR,
            "A cookie policy that ignores cookies.");

    public static final PropertyDescriptor REDIRECT_COOKIE_POLICY = new PropertyDescriptor.Builder()
            .name("redirect-cookie-policy")
            .displayName("Redirect Cookie Policy")
            .description("When a HTTP server responds to a request with a redirect, this is the cookie policy used to copy cookies to the following request.")
            .allowableValues(DEFAULT_COOKIE_POLICY, STANDARD_COOKIE_POLICY, STRICT_COOKIE_POLICY, NETSCAPE_COOKIE_POLICY, IGNORE_COOKIE_POLICY)
            .defaultValue(DEFAULT_COOKIE_POLICY_STR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files are transferred to the success relationship")
            .build();

    public static final String LAST_MODIFIED_DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

    // package access to enable unit testing
    static final String ETAG = "ETag";
    static final String LAST_MODIFIED = "LastModified";


    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private volatile List<PropertyDescriptor> customHeaders = new ArrayList<>();

    private final AtomicBoolean clearState = new AtomicBoolean(false);

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
        properties.add(REDIRECT_COOKIE_POLICY);
        properties.add(HTTPUtils.PROXY_CONFIGURATION_SERVICE);
        properties.add(PROXY_HOST);
        properties.add(PROXY_PORT);
        this.properties = Collections.unmodifiableList(properties);
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
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        clearState.set(true);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        if (clearState.getAndSet(false)) {
            context.getStateManager().clear(Scope.LOCAL);
        }
        if (customHeaders.size() == 0) {
            for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                // only add the custom defined Headers (i.e. dynamic properties)
                if (!getSupportedPropertyDescriptors().contains(property.getKey())) {
                    customHeaders.add(property.getKey());
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        if (context.getProperty(URL).evaluateAttributeExpressions().getValue().startsWith("https") && context.getProperty(SSL_CONTEXT_SERVICE).getValue() == null) {
            results.add(new ValidationResult.Builder()
                    .explanation("URL is set to HTTPS protocol but no SSLContext has been specified")
                    .valid(false)
                    .subject("SSL Context")
                    .build());
        }

        HTTPUtils.validateProxyProperties(context, results);

        return results;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .addValidator(Validator.VALID)
                .required(false)
                .dynamic(true)
                .build();
    }

    private SSLContext createSSLContext(final SSLContextService service)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {

        final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();

        if (StringUtils.isNotBlank(service.getTrustStoreFile())) {
            final KeyStore truststore = KeyStoreUtils.getTrustStore(service.getTrustStoreType());
            try (final InputStream in = new FileInputStream(new File(service.getTrustStoreFile()))) {
                truststore.load(in, service.getTrustStorePassword().toCharArray());
            }
            sslContextBuilder.loadTrustMaterial(truststore, new TrustSelfSignedStrategy());
        }

        if (StringUtils.isNotBlank(service.getKeyStoreFile())) {
            final KeyStore keystore = KeyStoreUtils.getKeyStore(service.getKeyStoreType());
            try (final InputStream in = new FileInputStream(new File(service.getKeyStoreFile()))) {
                keystore.load(in, service.getKeyStorePassword().toCharArray());
            }
            sslContextBuilder.loadKeyMaterial(keystore, service.getKeyStorePassword().toCharArray());
        }

        sslContextBuilder.useProtocol(service.getSslAlgorithm());

        return sslContextBuilder.build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ComponentLog logger = getLogger();

        final ProcessSession session = sessionFactory.createSession();
        final FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile != null) {
            session.transfer(incomingFlowFile, REL_SUCCESS);
            logger.warn("found FlowFile {} in input queue; transferring to success", new Object[]{incomingFlowFile});
        }

        // get the URL
        final String url = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        final URI uri;
        String source = url;
        try {
            uri = new URI(url);
            source = uri.getHost();
        } catch (final URISyntaxException swallow) {
            // this won't happen as the url has already been validated
        }

        // get the ssl context service
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        // create the connection manager
        final HttpClientConnectionManager conMan;
        if (sslContextService == null) {
            conMan = new BasicHttpClientConnectionManager();
        } else {
            final SSLContext sslContext;
            try {
                sslContext = createSSLContext(sslContextService);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);

            // Also include a plain socket factory for regular http connections (especially proxies)
            final Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslsf)
                            .register("http", PlainConnectionSocketFactory.getSocketFactory())
                            .build();

            conMan = new BasicHttpClientConnectionManager(socketFactoryRegistry);
        }

        try {
            // build the request configuration
            final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
            requestConfigBuilder.setConnectionRequestTimeout(context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            requestConfigBuilder.setConnectTimeout(context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            requestConfigBuilder.setSocketTimeout(context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            requestConfigBuilder.setRedirectsEnabled(context.getProperty(FOLLOW_REDIRECTS).asBoolean());
            switch (context.getProperty(REDIRECT_COOKIE_POLICY).getValue()) {
                case STANDARD_COOKIE_POLICY_STR:
                    requestConfigBuilder.setCookieSpec(CookieSpecs.STANDARD);
                    break;
                case STRICT_COOKIE_POLICY_STR:
                    requestConfigBuilder.setCookieSpec(CookieSpecs.STANDARD_STRICT);
                    break;
                case NETSCAPE_COOKIE_POLICY_STR:
                    requestConfigBuilder.setCookieSpec(CookieSpecs.NETSCAPE);
                    break;
                case IGNORE_COOKIE_POLICY_STR:
                    requestConfigBuilder.setCookieSpec(CookieSpecs.IGNORE_COOKIES);
                    break;
                case DEFAULT_COOKIE_POLICY_STR:
                default:
                    requestConfigBuilder.setCookieSpec(CookieSpecs.DEFAULT);
            }

            // build the http client
            final HttpClientBuilder clientBuilder = HttpClientBuilder.create();
            clientBuilder.setConnectionManager(conMan);

            // include the user agent
            final String userAgent = context.getProperty(USER_AGENT).getValue();
            if (userAgent != null) {
                clientBuilder.setUserAgent(userAgent);
            }

            // set the ssl context if necessary
            if (sslContextService != null) {
                clientBuilder.setSslcontext(sslContextService.createSSLContext(ClientAuth.REQUIRED));
            }

            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();

            // set the credentials if appropriate
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            if (username != null) {
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            }

            // Set the proxy if specified
            HTTPUtils.setProxy(context, clientBuilder, credentialsProvider);

            // create request
            final HttpGet get = new HttpGet(url);
            get.setConfig(requestConfigBuilder.build());

            final StateMap beforeStateMap;

            try {
                beforeStateMap = context.getStateManager().getState(Scope.LOCAL);
                final String lastModified = beforeStateMap.get(LAST_MODIFIED + ":" + url);
                if (lastModified != null) {
                    get.addHeader(HEADER_IF_MODIFIED_SINCE, parseStateValue(lastModified).getValue());
                }

                final String etag = beforeStateMap.get(ETAG + ":" + url);
                if (etag != null) {
                    get.addHeader(HEADER_IF_NONE_MATCH, parseStateValue(etag).getValue());
                }
            } catch (final IOException ioe) {
                throw new ProcessException(ioe);
            }

            final String accept = context.getProperty(ACCEPT_CONTENT_TYPE).getValue();
            if (accept != null) {
                get.addHeader(HEADER_ACCEPT, accept);
            }

            // Add dynamic headers

            PropertyValue customHeaderValue;
            for (PropertyDescriptor customProperty : customHeaders) {
                customHeaderValue = context.getProperty(customProperty).evaluateAttributeExpressions();
                if (StringUtils.isNotBlank(customHeaderValue.getValue())) {
                    get.addHeader(customProperty.getName(), customHeaderValue.getValue());
                }
            }


            // create the http client
            try (final CloseableHttpClient client = clientBuilder.build()) {
                // NOTE: including this inner try in order to swallow exceptions on close
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

                    if ((statusCode >= 300) || (statusCode == 204)) {
                        logger.error("received status code {}:{} from {}", new Object[]{statusCode, statusExplanation, url});
                        // doing a commit in case there were flow files in the input queue
                        session.commit();
                        return;
                    }

                    FlowFile flowFile = session.create();
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), context.getProperty(FILENAME).evaluateAttributeExpressions().getValue());
                    flowFile = session.putAttribute(flowFile, this.getClass().getSimpleName().toLowerCase() + ".remote.source", source);
                    flowFile = session.importFrom(response.getEntity().getContent(), flowFile);

                    final Header contentTypeHeader = response.getFirstHeader("Content-Type");
                    if (contentTypeHeader != null) {
                        final String contentType = contentTypeHeader.getValue();
                        if (!contentType.trim().isEmpty()) {
                            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), contentType.trim());
                        }
                    }

                    final long flowFileSize = flowFile.getSize();
                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(flowFileSize);
                    session.getProvenanceReporter().receive(flowFile, url, stopWatch.getDuration(TimeUnit.MILLISECONDS));
                    session.transfer(flowFile, REL_SUCCESS);
                    logger.info("Successfully received {} from {} at a rate of {}; transferred to success", new Object[]{flowFile, url, dataRate});
                    session.commit();

                    updateStateMap(context, response, beforeStateMap, url);

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
            } catch (final IOException e) {
                logger.debug("Error closing client due to {}, continuing.", new Object[]{e.getMessage()});
            }
        } finally {
            conMan.shutdown();
        }
    }

    private void updateStateMap(ProcessContext context, HttpResponse response, StateMap beforeStateMap, String url) {
        try {
            Map<String, String> workingMap = new HashMap<>();
            workingMap.putAll(beforeStateMap.toMap());
            final StateManager stateManager = context.getStateManager();
            StateMap oldValue = beforeStateMap;

            long currentTime = System.currentTimeMillis();

            final Header receivedLastModified = response.getFirstHeader(HEADER_LAST_MODIFIED);
            if (receivedLastModified != null) {
                workingMap.put(LAST_MODIFIED + ":" + url, currentTime + ":" + receivedLastModified.getValue());
            }

            final Header receivedEtag = response.getFirstHeader(HEADER_ETAG);
            if (receivedEtag != null) {
                workingMap.put(ETAG + ":" + url, currentTime + ":" + receivedEtag.getValue());
            }

            boolean replaceSucceeded = stateManager.replace(oldValue, workingMap, Scope.LOCAL);
            boolean changed;

            while (!replaceSucceeded) {
                oldValue = stateManager.getState(Scope.LOCAL);
                workingMap.clear();
                workingMap.putAll(oldValue.toMap());

                changed = false;

                if (receivedLastModified != null) {
                    Tuple<String, String> storedLastModifiedTuple = parseStateValue(workingMap.get(LAST_MODIFIED + ":" + url));

                    if (Long.parseLong(storedLastModifiedTuple.getKey()) < currentTime) {
                        workingMap.put(LAST_MODIFIED + ":" + url, currentTime + ":" + receivedLastModified.getValue());
                        changed = true;
                    }
                }

                if (receivedEtag != null) {
                    Tuple<String, String> storedLastModifiedTuple = parseStateValue(workingMap.get(ETAG + ":" + url));

                    if (Long.parseLong(storedLastModifiedTuple.getKey()) < currentTime) {
                        workingMap.put(ETAG + ":" + url, currentTime + ":" + receivedEtag.getValue());
                        changed = true;
                    }
                }

                if (changed) {
                    replaceSucceeded = stateManager.replace(oldValue, workingMap, Scope.LOCAL);
                } else {
                    break;
                }
            }
        } catch (final IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    protected static Tuple<String, String> parseStateValue(String mapValue) {
        int indexOfColon = mapValue.indexOf(":");

        String timestamp = mapValue.substring(0, indexOfColon);
        String value = mapValue.substring(indexOfColon + 1);
        return new Tuple<>(timestamp, value);
    }
}
