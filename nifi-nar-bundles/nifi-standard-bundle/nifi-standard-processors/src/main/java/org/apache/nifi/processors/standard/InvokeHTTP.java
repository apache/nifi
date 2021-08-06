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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Principal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.Cache;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.Handshake;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.MultipartBody.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ProxyAuthenticator;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.StreamUtils;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@SupportsBatching
@Tags({"http", "https", "rest", "client"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("An HTTP client processor which can interact with a configurable HTTP Endpoint. The destination URL and HTTP Method are configurable."
        + " FlowFile attributes are converted to HTTP headers and the FlowFile contents are included as the body of the request (if the HTTP Method is PUT, POST or PATCH).")
@WritesAttributes({
        @WritesAttribute(attribute = InvokeHTTP.STATUS_CODE, description = "The status code that is returned"),
        @WritesAttribute(attribute = InvokeHTTP.STATUS_MESSAGE, description = "The status message that is returned"),
        @WritesAttribute(attribute = InvokeHTTP.RESPONSE_BODY, description = "In the instance where the status code received is not a success (2xx) "
                + "then the response body will be put to the 'invokehttp.response.body' attribute of the request FlowFile."),
        @WritesAttribute(attribute = InvokeHTTP.REQUEST_URL, description = "The original request URL"),
        @WritesAttribute(attribute = InvokeHTTP.RESPONSE_URL, description = "The URL that was ultimately requested after any redirects were followed"),
        @WritesAttribute(attribute = InvokeHTTP.TRANSACTION_ID, description = "The transaction ID that is returned after reading the response"),
        @WritesAttribute(attribute = InvokeHTTP.REMOTE_DN, description = "The DN of the remote server"),
        @WritesAttribute(attribute = InvokeHTTP.EXCEPTION_CLASS, description = "The Java exception class raised when the processor fails"),
        @WritesAttribute(attribute = InvokeHTTP.EXCEPTION_MESSAGE, description = "The Java exception message raised when the processor fails"),
        @WritesAttribute(attribute = "user-defined", description = "If the 'Put Response Body In Attribute' property is set then whatever it is set to "
                + "will become the attribute key and the value would be the body of the HTTP response.")})
@DynamicProperties({
        @DynamicProperty(name = "Header Name", value = "Attribute Expression Language", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description =
                        "Send request header with a key matching the Dynamic Property Key and a value created by evaluating "
                                + "the Attribute Expression Language set in the value of the Dynamic Property."),
        @DynamicProperty(name = "post:form:<NAME>", value = "Attribute Expression Language", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description =
                        "When the HTTP Method is POST, dynamic properties with the property name in the form of post:form:<NAME>,"
                                + " where the <NAME> will be the form data name, will be used to fill out the multipart form parts."
                                + "  If send message body is false, the flowfile will not be sent, but any other form data will be.")
})
public class InvokeHTTP extends AbstractProcessor {
    public final static String STATUS_CODE = "invokehttp.status.code";
    public final static String STATUS_MESSAGE = "invokehttp.status.message";
    public final static String RESPONSE_BODY = "invokehttp.response.body";
    public final static String REQUEST_URL = "invokehttp.request.url";
    public final static String RESPONSE_URL = "invokehttp.response.url";
    public final static String TRANSACTION_ID = "invokehttp.tx.id";
    public final static String REMOTE_DN = "invokehttp.remote.dn";
    public final static String EXCEPTION_CLASS = "invokehttp.java.exception.class";
    public final static String EXCEPTION_MESSAGE = "invokehttp.java.exception.message";

    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    public static final String FORM_BASE = "post:form";

    // Set of flowfile attributes which we generally always ignore during
    // processing, including when converting http headers, copying attributes, etc.
    // This set includes our strings defined above as well as some standard flowfile
    // attributes.
    public static final Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            STATUS_CODE, STATUS_MESSAGE, RESPONSE_BODY, REQUEST_URL, RESPONSE_URL, TRANSACTION_ID, REMOTE_DN,
            EXCEPTION_CLASS, EXCEPTION_MESSAGE,
            "uuid", "filename", "path")));

    public static final String HTTP = "http";
    public static final String HTTPS = "https";

    private static final Pattern DYNAMIC_FORM_PARAMETER_NAME = Pattern.compile("post:form:(?<formDataName>.*)$");
    private static final String FORM_DATA_NAME_GROUP = "formDataName";

    // properties
    public static final PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
            .name("HTTP Method")
            .description("HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). Arbitrary methods are also supported. "
                    + "Methods other than POST, PUT and PATCH will be sent without a message body.")
            .required(true)
            .defaultValue("GET")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("Remote URL")
            .description("Remote URL which will be connected to, including scheme, host, port, path.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final PropertyDescriptor PROP_IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idle-timeout")
            .displayName("Idle Timeout")
            .description("Max idle time before closing connection to the remote service.")
            .required(true)
            .defaultValue("5 mins")
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.MILLISECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .build();

    public static final PropertyDescriptor PROP_MAX_IDLE_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("max-idle-connections")
            .displayName("Max Idle Connections")
            .description("Max number of idle connections to keep open.")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
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

    public static final PropertyDescriptor PROP_USERAGENT = new PropertyDescriptor.Builder()
            .name("Useragent")
            .displayName("Useragent")
            .description("The Useragent identifier sent along with each request")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections."
                    + " It is also used to connect to HTTPS Proxy.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor PROP_PROXY_TYPE = new PropertyDescriptor.Builder()
            .name("Proxy Type")
            .displayName("Proxy Type")
            .description("The type of the proxy we are connecting to. Must be either " + HTTP + " or " + HTTPS)
            .defaultValue(HTTP)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_PROXY_USER = new PropertyDescriptor.Builder()
            .name("invokehttp-proxy-user")
            .displayName("Proxy Username")
            .description("Username to set when authenticating against proxy")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("invokehttp-proxy-password")
            .displayName("Proxy Password")
            .description("Password to set when authenticating against proxy")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content-Type")
            .description("The Content-Type to specify for when content is being transmitted through a PUT, POST or PATCH. "
                    + "In the case of an empty value after evaluating an expression language expression, Content-Type defaults to " + DEFAULT_CONTENT_TYPE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final PropertyDescriptor PROP_FORM_BODY_FORM_NAME = new PropertyDescriptor.Builder()
            .name("form-body-form-name")
            .displayName("FlowFile Form Data Name")
            .description("When Send Message Body is true, and FlowFile Form Data Name is set, "
                    + " the FlowFile will be sent as the message body in multipart/form format with this value "
                    + "as the form data name.")
            .required(false)
            .addValidator(
                    StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_SET_FORM_FILE_NAME = new PropertyDescriptor.Builder()
            .name("set-form-filename")
            .displayName("Set FlowFile Form Data File Name")
            .description(
                    "When Send Message Body is true, FlowFile Form Data Name is set, "
                            + "and Set FlowFile Form Data File Name is true, the FlowFile's fileName value "
                            + "will be set as the filename property of the form data.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final PropertyDescriptor PROP_USE_ETAG = new PropertyDescriptor.Builder()
            .name("use-etag")
            .description("Enable HTTP entity tag (ETag) support for HTTP requests.")
            .displayName("Use HTTP ETag")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_ETAG_MAX_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("etag-max-cache-size")
            .description("The maximum size that the ETag cache should be allowed to grow to. The default size is 10MB.")
            .displayName("Maximum ETag Cache Size")
            .required(true)
            .defaultValue("10MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_RESPONSE_CONTENT = new PropertyDescriptor.Builder()
            .name("ignore-response-content")
            .description("If true, the processor will not write the response's content into the flow file.")
            .displayName("Ignore response's content")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor DISABLE_HTTP2_PROTOCOL = new PropertyDescriptor.Builder()
            .name("disable-http2")
            .description("Determines whether or not to disable use of the HTTP/2 protocol version. If disabled, only HTTP/1.1 is supported.")
            .displayName("Disable HTTP/2")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_METHOD,
            PROP_URL,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_CONNECT_TIMEOUT,
            PROP_READ_TIMEOUT,
            PROP_IDLE_TIMEOUT,
            PROP_MAX_IDLE_CONNECTIONS,
            PROP_DATE_HEADER,
            PROP_FOLLOW_REDIRECTS,
            DISABLE_HTTP2_PROTOCOL,
            PROP_ATTRIBUTES_TO_SEND,
            PROP_USERAGENT,
            PROP_BASIC_AUTH_USERNAME,
            PROP_BASIC_AUTH_PASSWORD,
            PROXY_CONFIGURATION_SERVICE,
            PROP_PROXY_HOST,
            PROP_PROXY_PORT,
            PROP_PROXY_TYPE,
            PROP_PROXY_USER,
            PROP_PROXY_PASSWORD,
            PROP_PUT_OUTPUT_IN_ATTRIBUTE,
            PROP_PUT_ATTRIBUTE_MAX_LENGTH,
            PROP_DIGEST_AUTH,
            PROP_OUTPUT_RESPONSE_REGARDLESS,
            PROP_ADD_HEADERS_TO_REQUEST,
            PROP_CONTENT_TYPE,
            PROP_SEND_BODY,
            PROP_USE_CHUNKED_ENCODING,
            PROP_PENALIZE_NO_RETRY,
            PROP_USE_ETAG,
            PROP_ETAG_MAX_CACHE_SIZE,
            IGNORE_RESPONSE_CONTENT,
            PROP_FORM_BODY_FORM_NAME,
            PROP_SET_FORM_FILE_NAME));

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

    // RFC 2616 Date Time Formatter with hard-coded GMT Zone
    // https://tools.ietf.org/html/rfc2616#section-3.3 - date format header should not be localized
    private static final DateTimeFormatter RFC_2616_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);

    // Multiple Header Delimiter
    private static final String MULTIPLE_HEADER_DELIMITER = ", ";

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    private volatile Pattern regexAttributesToSend = null;

    private volatile boolean useChunked = false;

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith(FORM_BASE)) {

            Matcher matcher = DYNAMIC_FORM_PARAMETER_NAME.matcher(propertyDescriptorName);
            if (matcher.matches()) {
                return new PropertyDescriptor.Builder()
                        .required(false)
                        .name(propertyDescriptorName)
                        .description("Form Data " + matcher.group(FORM_DATA_NAME_GROUP))
                        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                        .dynamic(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .build();
            }
            return null;
        }

        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

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
        if (proxyUserSet && !proxyHostSet) {
            results.add(new ValidationResult.Builder().subject("Proxy").valid(false).explanation("If Proxy username is set, proxy host must be set").build());
        }

        final String proxyType = validationContext.getProperty(PROP_PROXY_TYPE).evaluateAttributeExpressions().getValue();

        if (!HTTP.equals(proxyType) && !HTTPS.equals(proxyType)) {
            results.add(new ValidationResult.Builder().subject(PROP_PROXY_TYPE.getDisplayName()).valid(false)
                    .explanation(PROP_PROXY_TYPE.getDisplayName() + " must be either " + HTTP + " or " + HTTPS).build());
        }

        if (HTTPS.equals(proxyType)
                && !validationContext.getProperty(PROP_SSL_CONTEXT_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder().subject("SSL Context Service").valid(false).explanation("If Proxy Type is HTTPS, SSL Context Service must be set").build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, results, PROXY_SPECS);

        // Check for dynamic properties for form components.
        // Even if the flowfile is not sent, we may still send form parameters.
        boolean hasFormData = false;
        for (final PropertyDescriptor descriptor : validationContext.getProperties().keySet()) {
            final Matcher matcher = DYNAMIC_FORM_PARAMETER_NAME.matcher(descriptor.getName());
            if (matcher.matches()) {
                hasFormData = true;
                break;
            }
        }

        // if form data exists, and send body is true, Flowfile Form Data Name must be set.
        final boolean sendBody = validationContext.getProperty(PROP_SEND_BODY).asBoolean();
        final boolean contentNameSet = validationContext.getProperty(PROP_FORM_BODY_FORM_NAME).isSet();
        if (hasFormData) {
            if (sendBody && !contentNameSet) {
                final String explanation = String.format("[%s] is required when Form Data properties are configured and [%s] is enabled",
                        PROP_FORM_BODY_FORM_NAME.getDisplayName(),
                        PROP_SEND_BODY.getDisplayName());
                results.add(new ValidationResult.Builder()
                        .subject(PROP_FORM_BODY_FORM_NAME.getDisplayName())
                        .valid(false)
                        .explanation(explanation)
                        .build());
            }
        }
        if (!sendBody && contentNameSet) {
            final String explanation = String.format("[%s] must be [true] when Form Data properties are configured and [%s] is configured",
                    PROP_SEND_BODY.getDisplayName(),
                    PROP_FORM_BODY_FORM_NAME.getDisplayName());
            results.add(new ValidationResult.Builder()
                    .subject(PROP_FORM_BODY_FORM_NAME.getDisplayName())
                    .valid(false)
                    .explanation(explanation)
                    .build());
        }

        return results;
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context) throws TlsException, IOException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
            final String proxyHost = context.getProperty(PROP_PROXY_HOST).evaluateAttributeExpressions().getValue();
            final Integer proxyPort = context.getProperty(PROP_PROXY_PORT).evaluateAttributeExpressions().asInteger();
            if (proxyHost != null && proxyPort != null) {
                componentProxyConfig.setProxyType(Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                final String proxyUsername = trimToEmpty(context.getProperty(PROP_PROXY_USER).evaluateAttributeExpressions().getValue());
                final String proxyPassword = context.getProperty(PROP_PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
                componentProxyConfig.setProxyUserName(proxyUsername);
                componentProxyConfig.setProxyUserPassword(proxyPassword);
            }
            return componentProxyConfig;
        });

        final Proxy proxy = proxyConfig.createProxy();
        if (!Type.DIRECT.equals(proxy.type())) {
            okHttpClientBuilder.proxy(proxy);
            if (proxyConfig.hasCredential()) {
                ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
            }
        }

        // configure ETag cache if enabled
        final boolean etagEnabled = context.getProperty(PROP_USE_ETAG).asBoolean();
        if (etagEnabled) {
            final int maxCacheSizeBytes = context.getProperty(PROP_ETAG_MAX_CACHE_SIZE).asDataSize(DataUnit.B).intValue();
            okHttpClientBuilder.cache(new Cache(getETagCacheDir(), maxCacheSizeBytes));
        }

        // Configure whether HTTP/2 protocol should be disabled
        if (context.getProperty(DISABLE_HTTP2_PROTOCOL).asBoolean()) {
            okHttpClientBuilder.protocols(Collections.singletonList(Protocol.HTTP_1_1));
        }

        // Set timeouts
        okHttpClientBuilder.connectTimeout((context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(context.getProperty(PROP_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);

        // Set connectionpool limits
        okHttpClientBuilder.connectionPool(
                new ConnectionPool(
                        context.getProperty(PROP_MAX_IDLE_CONNECTIONS).asInteger(),
                        context.getProperty(PROP_IDLE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS
                )
        );

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(context.getProperty(PROP_FOLLOW_REDIRECTS).asBoolean());

        // Apply the TLS configuration if present
        final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslService != null) {
            final SSLContext sslContext = sslService.createContext();
            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            final TlsConfiguration tlsConfiguration = sslService.createTlsConfiguration();
            final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
            okHttpClientBuilder.sslSocketFactory(socketFactory, trustManager);
        }

        setAuthenticator(okHttpClientBuilder, context);

        useChunked = context.getProperty(PROP_USE_CHUNKED_ENCODING).asBoolean();

        okHttpClientAtomicReference.set(okHttpClientBuilder.build());
    }

    private void setAuthenticator(OkHttpClient.Builder okHttpClientBuilder, ProcessContext context) {
        final String authUser = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).getValue());

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

            okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
            okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        FlowFile requestFlowFile = session.get();

        // Checking to see if the property to put the body of the response in an attribute was set
        boolean putToAttribute = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).isSet();
        if (requestFlowFile == null) {
            if (context.hasNonLoopConnection()) {
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
            final String urlProperty = trimToEmpty(context.getProperty(PROP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());
            final URL url = new URL(urlProperty);

            Request httpRequest = configureRequest(context, session, requestFlowFile, url);

            // log request
            logRequest(logger, httpRequest);

            // emit send provenance event if successfully sent to the server
            if (httpRequest.body() != null) {
                session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
            }

            final long startNanos = System.nanoTime();

            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
                // output the raw response headers (DEBUG level only)
                logResponse(logger, url, responseHttp);

                // store the status code and message
                int statusCode = responseHttp.code();
                String statusMessage = responseHttp.message();

                // Create a map of the status attributes that are always written to the request and response FlowFiles
                Map<String, String> statusAttributes = new HashMap<>();
                statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
                statusAttributes.put(STATUS_MESSAGE, statusMessage);
                statusAttributes.put(REQUEST_URL, url.toExternalForm());
                statusAttributes.put(RESPONSE_URL, responseHttp.request().url().toString());
                statusAttributes.put(TRANSACTION_ID, txId.toString());

                if (requestFlowFile != null) {
                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
                }

                // If the property to add the response headers to the request flowfile is true then add them
                if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean() && requestFlowFile != null) {
                    // write the response headers as attributes
                    // this will overwrite any existing flowfile attributes
                    requestFlowFile = session.putAllAttributes(requestFlowFile, convertAttributesFromHeaders(responseHttp));
                }

                boolean outputBodyToRequestAttribute = (!isSuccess(statusCode) || putToAttribute) && requestFlowFile != null;
                boolean outputBodyToResponseContent = (isSuccess(statusCode) && !putToAttribute) || context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean();
                ResponseBody responseBody = responseHttp.body();
                boolean bodyExists = responseBody != null && !context.getProperty(IGNORE_RESPONSE_CONTENT).asBoolean();

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
                        responseFlowFile = session.putAllAttributes(responseFlowFile, convertAttributesFromHeaders(responseHttp));

                        // transfer the message body to the payload
                        // can potentially be null in edge cases
                        if (bodyExists) {
                            // write content type attribute to response flowfile if it is available
                            final MediaType contentType = responseBody.contentType();
                            if (contentType != null) {
                                responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), contentType.toString());
                            }
                            if (teeInputStream != null) {
                                responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
                            } else {
                                responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                            }

                            // emit provenance event
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            if (requestFlowFile != null) {
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
                    if (outputStreamToRequestAttribute != null) {
                        outputStreamToRequestAttribute.close();
                    }
                    if (teeInputStream != null) {
                        teeInputStream.close();
                    } else if (responseBodyStream != null) {
                        responseBodyStream.close();
                    }
                }

                route(requestFlowFile, responseFlowFile, session, context, statusCode);

            }
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
            if (responseFlowFile != null) {
                session.remove(responseFlowFile);
            }
        }
    }


    private Request configureRequest(final ProcessContext context, final ProcessSession session, final FlowFile requestFlowFile, URL url) {
        final Request.Builder requestBuilder = new Request.Builder();

        requestBuilder.url(url);
        final String authUser = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && "false".equalsIgnoreCase(context.getProperty(PROP_DIGEST_AUTH).getValue())) {
            final String authPass = trimToEmpty(context.getProperty(PROP_BASIC_AUTH_PASSWORD).getValue());

            String credential = Credentials.basic(authUser, authPass);
            requestBuilder.header("Authorization", credential);
        }

        // set the request method
        String method = trimToEmpty(context.getProperty(PROP_METHOD).evaluateAttributeExpressions(requestFlowFile).getValue()).toUpperCase();
        switch (method) {
            case "GET":
                requestBuilder.get();
                break;
            case "POST":
                RequestBody requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder.post(requestBody);
                break;
            case "PUT":
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder.put(requestBody);
                break;
            case "PATCH":
                requestBody = getRequestBodyToSend(session, context, requestFlowFile);
                requestBuilder.patch(requestBody);
                break;
            case "HEAD":
                requestBuilder.head();
                break;
            case "DELETE":
                requestBuilder.delete();
                break;
            default:
                requestBuilder.method(method, null);
        }

        String userAgent = trimToEmpty(context.getProperty(PROP_USERAGENT).evaluateAttributeExpressions(requestFlowFile).getValue());
        requestBuilder.addHeader("User-Agent", userAgent);

        setHeaderProperties(context, requestBuilder, requestFlowFile);

        return requestBuilder.build();
    }

    private RequestBody getRequestBodyToSend(final ProcessSession session, final ProcessContext context,
                                             final FlowFile requestFlowFile) {

        boolean sendBody = context.getProperty(PROP_SEND_BODY).asBoolean();

        String evalContentType = context.getProperty(PROP_CONTENT_TYPE)
                .evaluateAttributeExpressions(requestFlowFile).getValue();
        final String contentType = StringUtils.isBlank(evalContentType) ? DEFAULT_CONTENT_TYPE : evalContentType;
        String contentKey = context.getProperty(PROP_FORM_BODY_FORM_NAME).evaluateAttributeExpressions(requestFlowFile).getValue();

        // Check for dynamic properties for form components.
        // Even if the flowfile is not sent, we may still send form parameters.
        Map<String, PropertyDescriptor> propertyDescriptors = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            Matcher matcher = DYNAMIC_FORM_PARAMETER_NAME.matcher(entry.getKey().getName());
            if (matcher.matches()) {
                propertyDescriptors.put(matcher.group(FORM_DATA_NAME_GROUP), entry.getKey());
            }
        }

        RequestBody requestBody = new RequestBody() {
            @Nullable
            @Override
            public MediaType contentType() {
                return MediaType.parse(contentType);
            }

            @Override
            public void writeTo(BufferedSink sink) {
                session.exportTo(requestFlowFile, sink.outputStream());
            }

            @Override
            public long contentLength() {
                return useChunked ? -1 : requestFlowFile.getSize();
            }
        };

        if (propertyDescriptors.size() > 0 || StringUtils.isNotEmpty(contentKey)) {
            // we have form data
            MultipartBody.Builder builder = new Builder().setType(MultipartBody.FORM);
            boolean useFileName = context.getProperty(PROP_SET_FORM_FILE_NAME).asBoolean();
            String contentFileName = null;
            if (useFileName) {
                contentFileName = requestFlowFile.getAttribute(CoreAttributes.FILENAME.key());
            }
            // loop through the dynamic form parameters
            for (final Map.Entry<String, PropertyDescriptor> entry : propertyDescriptors.entrySet()) {
                final String propValue = context.getProperty(entry.getValue().getName())
                        .evaluateAttributeExpressions(requestFlowFile).getValue();
                builder.addFormDataPart(entry.getKey(), propValue);
            }
            if (sendBody) {
                builder.addFormDataPart(contentKey, contentFileName, requestBody);
            }
            return builder.build();
        } else if (sendBody) {
            return requestBody;
        }
        return RequestBody.create(new byte[0], null);
    }

    private void setHeaderProperties(final ProcessContext context, final Request.Builder requestBuilder, final FlowFile requestFlowFile) {
        // check if we should send the a Date header with the request
        if (context.getProperty(PROP_DATE_HEADER).asBoolean()) {
            final ZonedDateTime universalCoordinatedTimeNow = ZonedDateTime.now(ZoneOffset.UTC);
            requestBuilder.addHeader("Date", RFC_2616_DATE_TIME.format(universalCoordinatedTimeNow));
        }

        for (String headerKey : dynamicPropertyNames) {
            String headerValue = context.getProperty(headerKey).evaluateAttributeExpressions(requestFlowFile).getValue();

            // don't include dynamic form data properties
            if (DYNAMIC_FORM_PARAMETER_NAME.matcher(headerKey).matches()) {
                continue;
            }

            requestBuilder.addHeader(headerKey, headerValue);
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
                    requestBuilder.addHeader(headerKey, headerVal);
                }
            }
        }
    }


    private void route(FlowFile request, FlowFile response, ProcessSession session, ProcessContext context, int statusCode) {
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
                request.url().url().toExternalForm(), getLogString(request.headers().toMultimap()));
    }

    private void logResponse(ComponentLog logger, URL url, Response response) {
        logger.debug("\nResponse from remote service:\n\t{}\n{}",
                url.toExternalForm(), getLogString(response.headers().toMultimap()));
    }

    private String getLogString(Map<String, List<String>> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            List<String> list = entry.getValue();
            if (!list.isEmpty()) {
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
        }
        return sb.toString();
    }

    /**
     * Returns a Map of flowfile attributes from the response http headers. Multivalue headers are naively converted to comma separated strings.
     */
    private Map<String, String> convertAttributesFromHeaders(final Response responseHttp) {
        // create a new hashmap to store the values from the connection
        final Map<String, String> attributes = new HashMap<>();
        final Headers headers = responseHttp.headers();
        headers.names().forEach((key) -> {
            final List<String> values = headers.values(key);
            // we ignore any headers with no actual values (rare)
            if (!values.isEmpty()) {
                // create a comma separated string from the values, this is stored in the map
                final String value = StringUtils.join(values, MULTIPLE_HEADER_DELIMITER);
                attributes.put(key, value);
            }
        });

        final Handshake handshake = responseHttp.handshake();
        if (handshake != null) {
            final Principal principal = handshake.peerPrincipal();
            if (principal != null) {
                attributes.put(REMOTE_DN, principal.getName());
            }
        }

        return attributes;
    }

    private Charset getCharsetFromMediaType(MediaType contentType) {
        return contentType != null ? contentType.charset(StandardCharsets.UTF_8) : StandardCharsets.UTF_8;
    }

    /**
     * Retrieve the directory in which OkHttp should cache responses. This method opts
     * to use a temp directory to write the cache, which means that the cache will be written
     * to a new location each time this processor is scheduled.
     * <p>
     * Ref: https://github.com/square/okhttp/wiki/Recipes#response-caching
     *
     * @return the directory in which the ETag cache should be written
     */
    private static File getETagCacheDir() throws IOException {
        return Files.createTempDirectory(InvokeHTTP.class.getSimpleName()).toFile();
    }
}
