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

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.tls.OkHostnameVerifier;
import okio.BufferedSink;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ProxyAuthenticator;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.stream.io.StreamUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@SupportsBatching
@Tags({"http", "https", "rest", "client"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("An HTTP client processor which can interact with a configurable HTTP Endpoint. The destination URL and HTTP Method are configurable."
    + " FlowFile attributes are converted to HTTP headers and the FlowFile contents are included as the body of the request (if the HTTP Method is PUT, POST or PATCH).")
@WritesAttributes({
    @WritesAttribute(attribute = "invokehttp.status.code", description = "The status code that is returned"),
    @WritesAttribute(attribute = "invokehttp.status.message", description = "The status message that is returned"),
    @WritesAttribute(attribute = "invokehttp.response.body", description = "In the instance where the status code received is not a success (2xx) "
        + "then the response body will be put to the 'invokehttp.response.body' attribute of the request FlowFile."),
    @WritesAttribute(attribute = "invokehttp.request.url", description = "The request URL"),
    @WritesAttribute(attribute = "invokehttp.tx.id", description = "The transaction ID that is returned after reading the response"),
    @WritesAttribute(attribute = "invokehttp.remote.dn", description = "The DN of the remote server"),
    @WritesAttribute(attribute = "invokehttp.java.exception.class", description = "The Java exception class raised when the processor fails"),
    @WritesAttribute(attribute = "invokehttp.java.exception.message", description = "The Java exception message raised when the processor fails"),
    @WritesAttribute(attribute = "user-defined", description = "If the 'Put Response Body In Attribute' property is set then whatever it is set to "
        + "will become the attribute key and the value would be the body of the HTTP response.")})
@DynamicProperty(name = "Header Name", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "Send request header "
        + "with a key matching the Dynamic Property Key and a value created by evaluating the Attribute Expression Language set in the value "
        + "of the Dynamic Property.")
public final class InvokeHTTP extends AbstractProcessor {

    // flowfile attribute keys returned after reading the response
    public final static String STATUS_CODE = "invokehttp.status.code";
    public final static String STATUS_MESSAGE = "invokehttp.status.message";
    public final static String RESPONSE_BODY = "invokehttp.response.body";
    public final static String REQUEST_URL = "invokehttp.request.url";
    public final static String TRANSACTION_ID = "invokehttp.tx.id";
    public final static String REMOTE_DN = "invokehttp.remote.dn";
    public final static String EXCEPTION_CLASS = "invokehttp.java.exception.class";
    public final static String EXCEPTION_MESSAGE = "invokehttp.java.exception.message";


    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    // Set of flowfile attributes which we generally always ignore during
    // processing, including when converting http headers, copying attributes, etc.
    // This set includes our strings defined above as well as some standard flowfile
    // attributes.
    public static final Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            STATUS_CODE, STATUS_MESSAGE, RESPONSE_BODY, REQUEST_URL, TRANSACTION_ID, REMOTE_DN,
            EXCEPTION_CLASS, EXCEPTION_MESSAGE,
            "uuid", "filename", "path")));

    // properties
    public static final PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
            .name("HTTP Method")
            .description("HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). Arbitrary methods are also supported. "
                + "Methods other than POST, PUT and PATCH will be sent without a message body.")
            .required(true)
            .defaultValue("GET")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("Remote URL")
            .description("Remote URL which will be connected to, including scheme, host, port, path.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to remote service.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Max wait time for response from remote service.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_DATE_HEADER = new PropertyDescriptor.Builder()
            .name("Include Date Header")
            .description("Include an RFC-2616 Date header in the request.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_FOLLOW_REDIRECTS = new PropertyDescriptor.Builder()
            .name("Follow Redirects")
            .description("Follow HTTP redirects issued by remote server.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_ATTRIBUTES_TO_SEND = new PropertyDescriptor.Builder()
            .name("Attributes to Send")
            .description("Regular expression that defines which attributes to send as HTTP headers in the request. "
                    + "If not defined, no attributes are sent as headers. Also any dynamic properties set will be sent as headers. "
                    + "The dynamic property key will be the header key and the dynamic property value will be interpreted as expression "
                    + "language will be the header value.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor PROP_PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PROXY_USER = new PropertyDescriptor.Builder()
            .name("invokehttp-proxy-user")
            .displayName("Proxy Username")
            .description("Username to set when authenticating against proxy")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("invokehttp-proxy-password")
            .displayName("Proxy Password")
            .description("Password to set when authenticating against proxy")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONTENT_TYPE = new PropertyDescriptor.Builder()
        .name("Content-Type")
        .description("The Content-Type to specify for when content is being transmitted through a PUT, POST or PATCH. "
            + "In the case of an empty value after evaluating an expression language expression, Content-Type defaults to " + DEFAULT_CONTENT_TYPE)
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
        .build();

    public static final PropertyDescriptor PROP_SEND_BODY = new PropertyDescriptor.Builder()
            .name("send-message-body")
            .displayName("Send Message Body")
            .description("If true, sends the HTTP message body on POST/PUT/PATCH requests (default).  If false, suppresses the message body and content-type header for these requests.")
            .defaultValue("true")
            .allowableValues("true", "false")
            .required(false)
            .build();

    // Per RFC 7235, 2617, and 2616.
    // basic-credentials = base64-user-pass
    // base64-user-pass = userid ":" password
    // userid = *<TEXT excluding ":">
    // password = *TEXT
    //
    // OCTET = <any 8-bit sequence of data>
    // CTL = <any US-ASCII control character (octets 0 - 31) and DEL (127)>
    // LWS = [CRLF] 1*( SP | HT )
    // TEXT = <any OCTET except CTLs but including LWS>
    //
    // Per RFC 7230, username & password in URL are now disallowed in HTTP and HTTPS URIs.
    public static final PropertyDescriptor PROP_BASIC_AUTH_USERNAME = new PropertyDescriptor.Builder()
            .name("Basic Authentication Username")
            .displayName("Basic Authentication Username")
            .description("The username to be used by the client to authenticate against the Remote URL.  Cannot include control characters (0-31), ':', or DEL (127).")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
            .build();

    public static final PropertyDescriptor PROP_BASIC_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("Basic Authentication Password")
            .displayName("Basic Authentication Password")
            .description("The password to be used by the client to authenticate against the Remote URL.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
            .build();

    public static final PropertyDescriptor PROP_PUT_OUTPUT_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Put Response Body In Attribute")
            .description("If set, the response body received back will be put into an attribute of the original FlowFile instead of a separate "
                    + "FlowFile. The attribute key to put to is determined by evaluating value of this property. ")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROP_PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Length To Put In Attribute")
            .description("If routing the response body to an attribute of the original (by setting the \"Put response body in attribute\" "
                    + "property or by receiving an error status code), the number of characters put to the attribute value will be at "
                    + "most this amount. This is important because attributes are held in memory and large attributes will quickly "
                    + "cause out of memory issues. If the output goes longer than this value, it will be truncated to fit. "
                    + "Consider making this smaller if able.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();

    public static final PropertyDescriptor PROP_DIGEST_AUTH = new PropertyDescriptor.Builder()
            .name("Digest Authentication")
            .displayName("Use Digest Authentication")
            .description("Whether to communicate with the website using Digest Authentication. 'Basic Authentication Username' and 'Basic Authentication Password' are used "
                    + "for authentication.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_RESPONSE_REGARDLESS = new PropertyDescriptor.Builder()
            .name("Always Output Response")
            .description("Will force a response FlowFile to be generated and routed to the 'Response' relationship regardless of what the server status code received is "
                    + "or if the processor is configured to put the server response body in the request attribute. In the later configuration a request FlowFile with the "
                    + "response body in the attribute and a typical response FlowFile will be emitted to their respective relationships.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_TRUSTED_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Trusted Hostname")
            .description("Bypass the normal truststore hostname verifier to allow the specified remote hostname as trusted. "
                    + "Enabling this property has MITM security implications, use wisely. Will still accept other connections based "
                    + "on the normal truststore hostname verifier. Only valid with SSL (HTTPS) connections.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor PROP_ADD_HEADERS_TO_REQUEST = new PropertyDescriptor.Builder()
            .name("Add Response Headers to Request")
            .description("Enabling this property saves all the response headers to the original request. This may be when the response headers are needed "
                    + "but a response is not generated due to the status code received.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_USE_CHUNKED_ENCODING = new PropertyDescriptor.Builder()
            .name("Use Chunked Encoding")
            .description("When POST'ing, PUT'ing or PATCH'ing content set this property to true in order to not pass the 'Content-length' header and instead send 'Transfer-Encoding' with "
                    + "a value of 'chunked'. This will enable the data transfer mechanism which was introduced in HTTP 1.1 to pass data of unknown lengths in chunks.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_PENALIZE_NO_RETRY = new PropertyDescriptor.Builder()
            .name("Penalize on \"No Retry\"")
            .description("Enabling this property will penalize FlowFiles that are routed to the \"No Retry\" relationship.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_METHOD,
            PROP_URL,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_CONNECT_TIMEOUT,
            PROP_READ_TIMEOUT,
            PROP_DATE_HEADER,
            PROP_FOLLOW_REDIRECTS,
            PROP_ATTRIBUTES_TO_SEND,
            PROP_BASIC_AUTH_USERNAME,
            PROP_BASIC_AUTH_PASSWORD,
            PROP_PROXY_HOST,
            PROP_PROXY_PORT,
            PROP_PROXY_USER,
            PROP_PROXY_PASSWORD,
            PROP_PUT_OUTPUT_IN_ATTRIBUTE,
            PROP_PUT_ATTRIBUTE_MAX_LENGTH,
            PROP_DIGEST_AUTH,
            PROP_OUTPUT_RESPONSE_REGARDLESS,
            PROP_TRUSTED_HOSTNAME,
            PROP_ADD_HEADERS_TO_REQUEST,
            PROP_CONTENT_TYPE,
            PROP_SEND_BODY,
            PROP_USE_CHUNKED_ENCODING,
            PROP_PENALIZE_NO_RETRY));

    // relationships
    public static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
            .name("Original")
            .description("The original FlowFile will be routed upon success (2xx status codes). It will have new attributes detailing the "
                    + "success of the request.")
            .build();

    public static final Relationship REL_RESPONSE = new Relationship.Builder()
            .name("Response")
            .description("A Response FlowFile will be routed upon success (2xx status codes). If the 'Output Response Regardless' property "
                    + "is true then the response will be sent to this relationship regardless of the status code received.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("Retry")
            .description("The original FlowFile will be routed on any status code that can be retried (5xx status codes). It will have new "
                    + "attributes detailing the request.")
            .build();

    public static final Relationship REL_NO_RETRY = new Relationship.Builder()
            .name("No Retry")
            .description("The original FlowFile will be routed on any status code that should NOT be retried (1xx, 3xx, 4xx status codes).  "
                    + "It will have new attributes detailing the request.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The original FlowFile will be routed on any type of connection failure, timeout or general exception. "
                    + "It will have new attributes detailing the request.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS_REQ, REL_RESPONSE, REL_RETRY, REL_NO_RETRY, REL_FAILURE)));

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    /**
     * Pattern used to compute RFC 2616 Dates (#sec3.3.1). This format is used by the HTTP Date header and is optionally sent by the processor. This date is effectively an RFC 822/1123 date
     * string, but HTTP requires it to be in GMT (preferring the literal 'GMT' string).
     */
    private static final String RFC_1123 = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(RFC_1123).withLocale(Locale.US).withZoneUTC();

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile Pattern regexAttributesToSend = null;
    private volatile boolean useChunked = false;

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        } else {
            // compile the attributes-to-send filter pattern
            if (PROP_ATTRIBUTES_TO_SEND.getName().equalsIgnoreCase(descriptor.getName())) {
                if (newValue == null || newValue.isEmpty()) {
                    regexAttributesToSend = null;
                } else {
                    final String trimmedValue = StringUtils.trimToEmpty(newValue);
                    regexAttributesToSend = Pattern.compile(trimmedValue);
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(3);
        final boolean proxyHostSet = validationContext.getProperty(PROP_PROXY_HOST).isSet();
        final boolean proxyPortSet = validationContext.getProperty(PROP_PROXY_PORT).isSet();

        if ((proxyHostSet && !proxyPortSet) || (!proxyHostSet && proxyPortSet)) {
            results.add(new ValidationResult.Builder().subject("Proxy Host and Port").valid(false).explanation("If Proxy Host or Proxy Port is set, both must be set").build());
        }

        final boolean proxyUserSet = validationContext.getProperty(PROP_PROXY_USER).isSet();
        final boolean proxyPwdSet = validationContext.getProperty(PROP_PROXY_PASSWORD).isSet();

        if ((proxyUserSet && !proxyPwdSet) || (!proxyUserSet && proxyPwdSet)) {
            results.add(new ValidationResult.Builder().subject("Proxy User and Password").valid(false).explanation("If Proxy Username or Proxy Password is set, both must be set").build());
        }
        if(proxyUserSet && !proxyHostSet) {
            results.add(new ValidationResult.Builder().subject("Proxy").valid(false).explanation("If Proxy username is set, proxy host must be set").build());
        }

        return results;
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context) throws IOException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();

        // Add a proxy if set
        final String proxyHost = context.getProperty(PROP_PROXY_HOST).getValue();
        final Integer proxyPort = context.getProperty(PROP_PROXY_PORT).asInteger();
        if (proxyHost != null && proxyPort != null) {
            final Proxy proxy = new Proxy(Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
            okHttpClientBuilder.proxy(proxy);
        }

        // Set timeouts
        okHttpClientBuilder.connectTimeout((context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(context.getProperty(PROP_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(context.getProperty(PROP_FOLLOW_REDIRECTS).asBoolean());

        final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createSSLContext(ClientAuth.NONE);

        // check if the ssl context is set and add the factory if so
        if (sslContext != null) {
            setSslSocketFactory(okHttpClientBuilder, sslService, sslContext);
        }

        // check the trusted hostname property and override the HostnameVerifier
        String trustedHostname = trimToEmpty(context.getProperty(PROP_TRUSTED_HOSTNAME).getValue());
        if (!trustedHostname.isEmpty()) {
            okHttpClientBuilder.hostnameVerifier(new OverrideHostnameVerifier(trustedHostname, OkHostnameVerifier.INSTANCE));
        }

        setAuthenticator(okHttpClientBuilder, context);

        useChunked = context.getProperty(PROP_USE_CHUNKED_ENCODING).asBoolean();

        okHttpClientAtomicReference.set(okHttpClientBuilder.build());
    }

    /*
        Overall, this method is based off of examples from OkHttp3 documentation:
            https://square.github.io/okhttp/3.x/okhttp/okhttp3/OkHttpClient.Builder.html#sslSocketFactory-javax.net.ssl.SSLSocketFactory-javax.net.ssl.X509TrustManager-
            https://github.com/square/okhttp/blob/master/samples/guide/src/main/java/okhttp3/recipes/CustomTrust.java#L156

        In-depth documentation on Java Secure Socket Extension (JSSE) Classes and interfaces:
            https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#JSSEClasses
     */
    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder, SSLContextService sslService, SSLContext sslContext)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {
        final String keystoreLocation = sslService.getKeyStoreFile();
        final String keystorePass = sslService.getKeyStorePassword();
        final String keystoreType = sslService.getKeyStoreType();

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = sslService.getTrustStoreFile();
        final String truststorePass = sslService.getTrustStorePassword();
        final String truststoreType = sslService.getTrustStoreType();

        KeyStore truststore = KeyStore.getInstance(truststoreType);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        truststore.load(new FileInputStream(truststoreLocation), truststorePass.toCharArray());
        trustManagerFactory.init(truststore);

        /*
            TrustManagerFactory.getTrustManagers returns a trust manager for each type of trust material. Since we are getting a trust manager factory that uses "X509"
            as it's trust management algorithm, we are able to grab the first (and thus the most preferred) and use it as our x509 Trust Manager

            https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/TrustManagerFactory.html#getTrustManagers--
         */
        final X509TrustManager x509TrustManager;
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers[0] != null) {
            x509TrustManager = (X509TrustManager) trustManagers[0];
        } else {
            throw new IllegalStateException("List of trust managers is null");
        }

        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);
    }

    private void setAuthenticator(OkHttpClient.Builder okHttpClientBuilder, ProcessContext context) {
        final String authUser = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).getValue());
        final String proxyUsername = trimToEmpty(context.getProperty(PROP_PROXY_USER).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && "true".equalsIgnoreCase(context.getProperty(PROP_DIGEST_AUTH).getValue())) {
            final String authPass = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_PASSWORD).getValue());

            /*
             * OkHttp doesn't have built-in Digest Auth Support. A ticket for adding it is here[1] but they authors decided instead to rely on a 3rd party lib.
             *
             * [1] https://github.com/square/okhttp/issues/205#issuecomment-154047052
             */
            final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(authUser, authPass);
            final DigestAuthenticator digestAuthenticator = new DigestAuthenticator(credentials);

            if(!proxyUsername.isEmpty()) {
                final String proxyPassword = context.getProperty(PROP_PROXY_PASSWORD).getValue();
                ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyUsername, proxyPassword);

                okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
            }

            okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
            okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
        } else {
            // Add proxy authentication only
            if(!proxyUsername.isEmpty()) {
                final String proxyPassword = context.getProperty(PROP_PROXY_PASSWORD).getValue();
                ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyUsername, proxyPassword);

                okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        FlowFile requestFlowFile = session.get();

        // Checking to see if the property to put the body of the response in an attribute was set
        boolean putToAttribute = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).isSet();
        if (requestFlowFile == null) {
            if(context.hasNonLoopConnection()){
                return;
            }

            String request = context.getProperty(PROP_METHOD).evaluateAttributeExpressions().getValue().toUpperCase();
            if ("POST".equals(request) || "PUT".equals(request) || "PATCH".equals(request)) {
                return;
            } else if (putToAttribute) {
                requestFlowFile = session.create();
            }
        }

        // Setting some initial variables
        final int maxAttributeSize = context.getProperty(PROP_PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
        final ComponentLog logger = getLogger();

        // Every request/response cycle has a unique transaction id which will be stored as a flowfile attribute.
        final UUID txId = UUID.randomUUID();

        FlowFile responseFlowFile = null;
        try {
            // read the url property from the context
            final String urlstr = trimToEmpty(context.getProperty(PROP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());
            final URL url = new URL(urlstr);

            Request httpRequest = configureRequest(context, session, requestFlowFile, url);

            // log request
            logRequest(logger, httpRequest);

            // emit send provenance event if successfully sent to the server
            if (httpRequest.body() != null) {
                session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
            }

            final long startNanos = System.nanoTime();
            Response responseHttp = okHttpClient.newCall(httpRequest).execute();

            // output the raw response headers (DEBUG level only)
            logResponse(logger, url, responseHttp);

            // store the status code and message
            int statusCode = responseHttp.code();
            String statusMessage = responseHttp.message();

            if (statusCode == 0) {
                throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
            }

            // Create a map of the status attributes that are always written to the request and response FlowFiles
            Map<String, String> statusAttributes = new HashMap<>();
            statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
            statusAttributes.put(STATUS_MESSAGE, statusMessage);
            statusAttributes.put(REQUEST_URL, url.toExternalForm());
            statusAttributes.put(TRANSACTION_ID, txId.toString());

            if (requestFlowFile != null) {
                requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
            }

            // If the property to add the response headers to the request flowfile is true then add them
            if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean() && requestFlowFile != null) {
                // write the response headers as attributes
                // this will overwrite any existing flowfile attributes
                requestFlowFile = session.putAllAttributes(requestFlowFile, convertAttributesFromHeaders(url, responseHttp));
            }

            boolean outputBodyToRequestAttribute = (!isSuccess(statusCode) || putToAttribute) && requestFlowFile != null;
            boolean outputBodyToResponseContent = (isSuccess(statusCode) && !putToAttribute) || context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean();
            ResponseBody responseBody = responseHttp.body();
            boolean bodyExists = responseBody != null;

            InputStream responseBodyStream = null;
            SoftLimitBoundedByteArrayOutputStream outputStreamToRequestAttribute = null;
            TeeInputStream teeInputStream = null;
            try {
                responseBodyStream = bodyExists ? responseBody.byteStream() : null;
                if (responseBodyStream != null && outputBodyToRequestAttribute && outputBodyToResponseContent) {
                    outputStreamToRequestAttribute = new SoftLimitBoundedByteArrayOutputStream(maxAttributeSize);
                    teeInputStream = new TeeInputStream(responseBodyStream, outputStreamToRequestAttribute);
                }

                if (outputBodyToResponseContent) {
                    /*
                     * If successful and putting to response flowfile, store the response body as the flowfile payload
                     * we include additional flowfile attributes including the response headers and the status codes.
                     */

                    // clone the flowfile to capture the response
                    if (requestFlowFile != null) {
                        responseFlowFile = session.create(requestFlowFile);
                    } else {
                        responseFlowFile = session.create();
                    }

                    // write attributes to response flowfile
                    responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);

                    // write the response headers as attributes
                    // this will overwrite any existing flowfile attributes
                    responseFlowFile = session.putAllAttributes(responseFlowFile, convertAttributesFromHeaders(url, responseHttp));

                    // transfer the message body to the payload
                    // can potentially be null in edge cases
                    if (bodyExists) {
                        // write content type attribute to response flowfile if it is available
                        if (responseBody.contentType() != null) {
                             responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), responseBody.contentType().toString());
                        }
                        if (teeInputStream != null) {
                            responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
                        } else {
                            responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                        }

                        // emit provenance event
                        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        if(requestFlowFile != null) {
                            session.getProvenanceReporter().fetch(responseFlowFile, url.toExternalForm(), millis);
                        } else {
                            session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);
                        }
                    }
                }

                // if not successful and request flowfile is not null, store the response body into a flowfile attribute
                if (outputBodyToRequestAttribute && bodyExists) {
                    String attributeKey = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).evaluateAttributeExpressions(requestFlowFile).getValue();
                    if (attributeKey == null) {
                        attributeKey = RESPONSE_BODY;
                    }
                    byte[] outputBuffer;
                    int size;

                    if (outputStreamToRequestAttribute != null) {
                        outputBuffer = outputStreamToRequestAttribute.getBuffer();
                        size = outputStreamToRequestAttribute.size();
                    } else {
                        outputBuffer = new byte[maxAttributeSize];
                        size = StreamUtils.fillBuffer(responseBodyStream, outputBuffer, false);
                    }
                    String bodyString = new String(outputBuffer, 0, size, getCharsetFromMediaType(responseBody.contentType()));
                    requestFlowFile = session.putAttribute(requestFlowFile, attributeKey, bodyString);

                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    session.getProvenanceReporter().modifyAttributes(requestFlowFile, "The " + attributeKey + " has been added. The value of which is the body of a http call to "
                            + url.toExternalForm() + ". It took " + millis + "millis,");
                }
            } finally {
                if(outputStreamToRequestAttribute != null){
                    outputStreamToRequestAttribute.close();
                    outputStreamToRequestAttribute = null;
                }
                if(teeInputStream != null){
                    teeInputStream.close();
                    teeInputStream = null;
                } else if(responseBodyStream != null){
                    responseBodyStream.close();
                    responseBodyStream = null;
                }
            }

            route(requestFlowFile, responseFlowFile, session, context, statusCode);
        } catch (final Exception e) {
            // penalize or yield
            if (requestFlowFile != null) {
                logger.error("Routing to {} due to exception: {}", new Object[]{REL_FAILURE.getName(), e}, e);
                requestFlowFile = session.penalize(requestFlowFile);
                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_CLASS, e.getClass().getName());
                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_MESSAGE, e.getMessage());
                // transfer original to failure
                session.transfer(requestFlowFile, REL_FAILURE);
            } else {
                logger.error("Yielding processor due to exception encountered as a source processor: {}", e);
                context.yield();
            }


            // cleanup response flowfile, if applicable
            try {
                if (responseFlowFile != null) {
                    session.remove(responseFlowFile);
                }
            } catch (final Exception e1) {
                logger.error("Could not cleanup response flowfile due to exception: {}", new Object[]{e1}, e1);
            }
        }
    }


    private Request configureRequest(final ProcessContext context, final ProcessSession session, final FlowFile requestFlowFile, URL url) {
        Request.Builder requestBuilder = new Request.Builder();

        requestBuilder = requestBuilder.url(url);
        final String authUser = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && "false".equalsIgnoreCase(context.getProperty(PROP_DIGEST_AUTH).getValue())) {
            final String authPass = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_PASSWORD).getValue());

            String credential = Credentials.basic(authUser, authPass);
            requestBuilder = requestBuilder.header("Authorization", credential);
        }

        // set the request method
        String method = trimToEmpty(context.getProperty(PROP_METHOD).evaluateAttributeExpressions(requestFlowFile).getValue()).toUpperCase();
        switch (method) {
            case "GET":
                requestBuilder = requestBuilder.get();
                break;
            case "POST":
                RequestBody requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder = requestBuilder.post(requestBody);
                break;
            case "PUT":
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder = requestBuilder.put(requestBody);
                break;
            case "PATCH":
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder = requestBuilder.patch(requestBody);
                break;
            case "HEAD":
                requestBuilder = requestBuilder.head();
                break;
            case "DELETE":
                requestBuilder = requestBuilder.delete();
                break;
            default:
                requestBuilder = requestBuilder.method(method, null);
        }

        requestBuilder = setHeaderProperties(context, requestBuilder, requestFlowFile);

        return requestBuilder.build();
    }

    private RequestBody getRequestBodyToSend(final ProcessSession session, final ProcessContext context, final FlowFile requestFlowFile) {
        if(context.getProperty(PROP_SEND_BODY).asBoolean()) {
            return new RequestBody() {
                @Override
                public MediaType contentType() {
                    String contentType = context.getProperty(PROP_CONTENT_TYPE).evaluateAttributeExpressions(requestFlowFile).getValue();
                    contentType = StringUtils.isBlank(contentType) ? DEFAULT_CONTENT_TYPE : contentType;
                    return MediaType.parse(contentType);
                }

                @Override
                public void writeTo(BufferedSink sink) throws IOException {
                    session.exportTo(requestFlowFile, sink.outputStream());
                }

                @Override
                public long contentLength(){
                    return useChunked ? -1 : requestFlowFile.getSize();
                }
            };
        } else {
            return RequestBody.create(null, new byte[0]);
        }
    }

    private Request.Builder setHeaderProperties(final ProcessContext context, Request.Builder requestBuilder, final FlowFile requestFlowFile) {
        // check if we should send the a Date header with the request
        if (context.getProperty(PROP_DATE_HEADER).asBoolean()) {
            requestBuilder = requestBuilder.addHeader("Date", DATE_FORMAT.print(System.currentTimeMillis()));
        }

        for (String headerKey : dynamicPropertyNames) {
            String headerValue = context.getProperty(headerKey).evaluateAttributeExpressions(requestFlowFile).getValue();
            requestBuilder = requestBuilder.addHeader(headerKey, headerValue);
        }

        // iterate through the flowfile attributes, adding any attribute that
        // matches the attributes-to-send pattern. if the pattern is not set
        // (it's an optional property), ignore that attribute entirely
        if (regexAttributesToSend != null && requestFlowFile != null) {
            Map<String, String> attributes = requestFlowFile.getAttributes();
            Matcher m = regexAttributesToSend.matcher("");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String headerKey = trimToEmpty(entry.getKey());

                // don't include any of the ignored attributes
                if (IGNORED_ATTRIBUTES.contains(headerKey)) {
                    continue;
                }

                // check if our attribute key matches the pattern
                // if so, include in the request as a header
                m.reset(headerKey);
                if (m.matches()) {
                    String headerVal = trimToEmpty(entry.getValue());
                    requestBuilder = requestBuilder.addHeader(headerKey, headerVal);
                }
            }
        }
        return requestBuilder;
    }


    private void route(FlowFile request, FlowFile response, ProcessSession session, ProcessContext context, int statusCode){
        // check if we should yield the processor
        if (!isSuccess(statusCode) && request == null) {
            context.yield();
        }

        // If the property to output the response flowfile regardless of status code is set then transfer it
        boolean responseSent = false;
        if (context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean()) {
            session.transfer(response, REL_RESPONSE);
            responseSent = true;
        }

        // transfer to the correct relationship
        // 2xx -> SUCCESS
        if (isSuccess(statusCode)) {
            // we have two flowfiles to transfer
            if (request != null) {
                session.transfer(request, REL_SUCCESS_REQ);
            }
            if (response != null && !responseSent) {
                session.transfer(response, REL_RESPONSE);
            }

            // 5xx -> RETRY
        } else if (statusCode / 100 == 5) {
            if (request != null) {
                request = session.penalize(request);
                session.transfer(request, REL_RETRY);
            }

            // 1xx, 3xx, 4xx -> NO RETRY
        } else {
            if (request != null) {
                if (context.getProperty(PROP_PENALIZE_NO_RETRY).asBoolean()) {
                    request = session.penalize(request);
                }
                session.transfer(request, REL_NO_RETRY);
            }
        }

    }

    private boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    private void logRequest(ComponentLog logger, Request request) {
        logger.debug("\nRequest to remote service:\n\t{}\n{}",
                new Object[]{request.url().url().toExternalForm(), getLogString(request.headers().toMultimap())});
    }

    private void logResponse(ComponentLog logger, URL url, Response response) {
        logger.debug("\nResponse from remote service:\n\t{}\n{}",
                new Object[]{url.toExternalForm(), getLogString(response.headers().toMultimap())});
    }

    private String getLogString(Map<String, List<String>> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            List<String> list = entry.getValue();
            if (list.isEmpty()) {
                continue;
            }
            sb.append("\t");
            sb.append(entry.getKey());
            sb.append(": ");
            if (list.size() == 1) {
                sb.append(list.get(0));
            } else {
                sb.append(list.toString());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Convert a collection of string values into a overly simple comma separated string.
     * <p/>
     * Does not handle the case where the value contains the delimiter. i.e. if a value contains a comma, this method does nothing to try and escape or quote the value, in traditional csv style.
     */
    private String csv(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        if (values.size() == 1) {
            return values.iterator().next();
        }

        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            value = value.trim();
            if (value.isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(value);
        }
        return sb.toString().trim();
    }

    /**
     * Returns a Map of flowfile attributes from the response http headers. Multivalue headers are naively converted to comma separated strings.
     */
    private Map<String, String> convertAttributesFromHeaders(URL url, Response responseHttp){
        // create a new hashmap to store the values from the connection
        Map<String, String> map = new HashMap<>();
        responseHttp.headers().names().forEach( (key) -> {
                if (key == null) {
                    return;
                }

                List<String> values = responseHttp.headers().values(key);

                // we ignore any headers with no actual values (rare)
                if (values == null || values.isEmpty()) {
                    return;
                }

                // create a comma separated string from the values, this is stored in the map
                String value = csv(values);

                // put the csv into the map
                map.put(key, value);
        });

        if ("HTTPS".equals(url.getProtocol().toUpperCase())) {
            map.put(REMOTE_DN, responseHttp.handshake().peerPrincipal().getName());
        }

        return map;
    }

    private Charset getCharsetFromMediaType(MediaType contentType) {
        return contentType != null ? contentType.charset(StandardCharsets.UTF_8) : StandardCharsets.UTF_8;
    }

    private static class OverrideHostnameVerifier implements HostnameVerifier {

        private final String trustedHostname;
        private final HostnameVerifier delegate;

        private OverrideHostnameVerifier(String trustedHostname, HostnameVerifier delegate) {
            this.trustedHostname = trustedHostname;
            this.delegate = delegate;
        }

        @Override
        public boolean verify(String hostname, SSLSession session) {
            if (trustedHostname.equalsIgnoreCase(hostname)) {
                return true;
            }
            return delegate.verify(hostname, session);
        }
    }
}
