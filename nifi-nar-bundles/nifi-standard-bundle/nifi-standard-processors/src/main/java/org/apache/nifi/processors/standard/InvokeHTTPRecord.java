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

import okhttp3.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;

import java.io.*;
import java.net.*;
import java.net.Proxy.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Principal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.MultipartBody.Builder;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.http.ContentEncodingStrategy;
import org.apache.nifi.processors.standard.http.CookieStrategy;
import org.apache.nifi.processors.standard.http.HttpHeader;
import org.apache.nifi.processors.standard.http.HttpMethod;
import org.apache.nifi.processors.standard.util.ProxyAuthenticator;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.StreamUtils;

@SupportsSensitiveDynamicProperties
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "schema", "json", "csv", "avro", "http", "https", "rest", "client"})
@CapabilityDescription("Takes a FlowFile containing record-oriented data and makes an HTTP request for each record, "
        + "storing the results in specified properties. Incoming FlowFiles can be routed based on whether any of the "
        + "HTTP requests fail, or can be split into two FlowFiles containing successful and failed records. Records "
        + "can also be classed as successful or failed based on the response body.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The generated FlowFile will have a 'record.count' "
                + "attribute indicating the number of records that were written to the FlowFile."),
        @WritesAttribute(attribute = "fragment.identifier", description = "All segments produced from the same parent "
                + "FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of "
                + "the segments that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of segments generated from the parent FlowFile"),
})
public class InvokeHTTPRecord extends AbstractProcessor {
    private enum HttpRequestResult {
        HTTP_SUCCESS,
        HTTP_CLIENT_ERROR,
        HTTP_SERVER_ERROR,
        HTTP_OTHER,
        RESPONSE_BODY_CRITERIA_FAILED,
        TIMEOUT,
        FAILURE
    }

    private volatile RecordPathCache recordPathCache;

    public final static String REMOTE_DN = "invokehttp.remote.dn";

    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    // RFC 2616 Date Time Formatter with hard-coded GMT Zone and US Locale. RFC 2616 Section 3.3 indicates the header should not be localized
    private static final DateTimeFormatter RFC_2616_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);

    private static final String MULTIPLE_HEADER_DELIMITER = ", ";

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    private volatile boolean chunkedTransferEncoding = false;

    private volatile Optional<OAuth2AccessTokenProvider> oauth2AccessTokenProviderOptional;

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    static final AllowableValue URL_TYPE_LITERAL = new AllowableValue("url-type-literal", "Literal Value",
        "The HTTP URL property is the HTTP remote URL including a scheme of http or https, as well as a hostname "
            + "or IP address with optional port and path elements (after Expression Language has been evaluated).");
    static final AllowableValue URL_TYPE_RECORD_PATH = new AllowableValue("url-type-record-path", "Record Path Value",
        "The HTTP URL property is a RecordPath that will be evaluated against each Record. The result should be the "
            + "HTTP remote URL including a scheme of http or https, a hostname or IP address, and optional port and "
            + "path elements (after Expression Language has been evaluated).");

    static final AllowableValue HEADER_TYPE_LITERAL = new AllowableValue("header-type-literal", "Literal Value",
        "Each dynamic property is a header, with the property name as the header name and the value as the literal header value.");
    static final AllowableValue HEADER_TYPE_RECORD_PATH = new AllowableValue("header-type-record-path", "Record Path Value",
        "Each dynamic property is a header, with the property name as the header name and a RecordPath as the value. "
            + "The RecordPath will be evaluated against each Record to determine the header value sent.");

    static final AllowableValue BODY_TYPE_LITERAL = new AllowableValue("body-type-literal", "Literal Value",
        "The Request Body property is used as the literal request body value.");
    static final AllowableValue BODY_TYPE_RECORD_PATH = new AllowableValue("body-type-record-path", "Record Path Value",
            "The Request Body property is a RecordPath that is evaluated against each Record to determine the request body sent.");

    static final AllowableValue ERROR_SPLIT_FAILURES = new AllowableValue("error-split-failures", "Split Failed Request Records",
            "Split any records with failing requests (after retries) into a new flowfile, routed to the failure relationship.");
    static final AllowableValue ERROR_FAIL_ENTIRE_FILE = new AllowableValue("error-fail-entire-file", "Route Entire FlowFile to Failure",
            "If a request fails for any records (after retries), route the entire flowfile to the failure relationship.");

    static final AllowableValue BODY_HANDLING_NONE = new AllowableValue("body-handling-none", "None",
            "Ignore the response body - do not add it to Records and do not use it to evaluate request success.");
    static final AllowableValue BODY_HANDLING_STRING = new AllowableValue("body-handling-string", "String",
            "If adding the response body to each input Record, treat it as a string.");
    static final AllowableValue BODY_HANDLING_RECORD = new AllowableValue("body-handling-record", "Record",
            "If adding the response body to each input Record, treat it as a Record. This requires a Response Body Record Reader "
            + "to be provided. Fields in the parsed Record will be merged with those at the Response Body Record Path if the "
            + "existing value at the path is a Record, otherwise the path will be overwritten by the response body Record.");

    static final String OP_EMPTY = "success-criteria-empty";
    static final String OP_NOT_EMPTY = "success-criteria-not-empty";
    static final String OP_LESS_THAN = "success-criteria-lt";
    static final String OP_LESS_THAN_OR_EQUAL = "success-criteria-lteq";
    static final String OP_EQUAL = "success-criteria-eq";
    static final String OP_NOT_EQUAL = "success-criteria-neq";
    static final String OP_GREATER_THAN_OR_EQUAL = "success-criteria-gteq";
    static final String OP_GREATER_THAN = "success-criteria-gt";

    static final AllowableValue SUCCESS_CRITERIA_EMPTY = new AllowableValue(OP_EMPTY, "Empty",
            "Success criteria RecordPath result is empty.");
    static final AllowableValue SUCCESS_CRITERIA_NOT_EMPTY = new AllowableValue(OP_NOT_EMPTY, "Not Empty",
            "Success criteria RecordPath result is not empty.");
    static final AllowableValue SUCCESS_CRITERIA_LESS_THAN = new AllowableValue(OP_LESS_THAN, "Less Than",
            "Success criteria RecordPath result less than comparison value.");
    static final AllowableValue SUCCESS_CRITERIA_LESS_THAN_OR_EQUAL = new AllowableValue(OP_LESS_THAN_OR_EQUAL, "Less Than or Equal",
            "Success criteria RecordPath result less than or equal to comparison value.");
    static final AllowableValue SUCCESS_CRITERIA_EQUAL = new AllowableValue(OP_EQUAL, "Equals",
            "Success criteria RecordPath result equal to comparison value.");
    static final AllowableValue SUCCESS_CRITERIA_NOT_EQUAL = new AllowableValue(OP_NOT_EQUAL, "Not Equal",
            "Success criteria RecordPath result is not equal to comparison value.");
    static final AllowableValue SUCCESS_CRITERIA_GREATER_THAN_OR_EQUAL = new AllowableValue(OP_GREATER_THAN_OR_EQUAL, "Greater Than or Equal",
            "Success criteria RecordPath result greater than or equal to comparison value.");
    static final AllowableValue SUCCESS_CRITERIA_GREATER_THAN = new AllowableValue(OP_GREATER_THAN, "Greater Than",
            "Success criteria RecordPath result greater than comparison value.");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor ERROR_ROUTING = new PropertyDescriptor.Builder()
        .name("error-routing")
        .displayName("Error Routing")
        .description("How to route records if an HTTP request error occurs.")
        .allowableValues(ERROR_FAIL_ENTIRE_FILE, ERROR_SPLIT_FAILURES)
        .defaultValue(ERROR_FAIL_ENTIRE_FILE.getValue())
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    public static final PropertyDescriptor HTTP_METHOD = new PropertyDescriptor.Builder()
        .name("HTTP Method")
        .description("HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). Arbitrary methods are also supported. "
                + "Methods other than POST, PUT and PATCH will be sent without a message body.")
        .required(true)
        .defaultValue(HttpMethod.GET.name())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
        .build();

    static final PropertyDescriptor HTTP_URL_TYPE = new PropertyDescriptor.Builder()
        .name("url-type")
        .displayName("HTTP URL Type")
        .description("Whether the HTTP URL property is a literal string or a RecordPath.")
        .allowableValues(URL_TYPE_LITERAL, URL_TYPE_RECORD_PATH)
        .defaultValue(URL_TYPE_LITERAL.getValue())
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    public static final PropertyDescriptor HTTP_URL_LITERAL = new PropertyDescriptor.Builder()
        .name("Remote URL Literal")
        .displayName("HTTP URL - Literal")
        .description("HTTP remote URL including a scheme of http or https, as well as a hostname or IP address with optional port and path elements.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .dependsOn(HTTP_URL_TYPE, URL_TYPE_LITERAL)
        .build();
    
    public static final PropertyDescriptor HTTP_URL_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("Remote URL RecordPath")
        .displayName("HTTP URL - RecordPath")
        .description("RecordPath to be evaluated against each record. Must evaluate to an HTTP remote URL including a scheme of http or https, "
            + "as well as a hostname or IP address with optional port and path elements.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(new RecordPathValidator())
        .dependsOn(HTTP_URL_TYPE, URL_TYPE_RECORD_PATH)
        .build();

    static final PropertyDescriptor REQUEST_BODY_TYPE = new PropertyDescriptor.Builder()
        .name("body-type")
        .displayName("Request Body Type")
        .description("Whether the Request Body field is interpreted as a literal string or as a RecordPath.")
        .allowableValues(BODY_TYPE_LITERAL, BODY_TYPE_RECORD_PATH)
        .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
        .defaultValue(BODY_TYPE_LITERAL.getValue())
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    public static final PropertyDescriptor REQUEST_BODY_LITERAL = new PropertyDescriptor.Builder()
        .name("Request Body Literal")
        .displayName("Request Body - Literal")
        .description("Body to send with the HTTP request.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
        .dependsOn(REQUEST_BODY_TYPE, BODY_TYPE_LITERAL)
        .build();

    public static final PropertyDescriptor REQUEST_BODY_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("Request Body RecordPath")
        .displayName("Request Body - RecordPath")
        .description("RecordPath to be evaluated against each record, with the result being used as the request body.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(new RecordPathValidator())
        .dependsOn(REQUEST_BODY_TYPE, BODY_TYPE_RECORD_PATH)
        .build();

    public static final PropertyDescriptor HTTP2_DISABLED = new PropertyDescriptor.Builder()
        .name("disable-http2")
        .displayName("HTTP/2 Disabled")
        .description("Disable negotiation of HTTP/2 protocol. HTTP/2 requires TLS. HTTP/1.1 protocol supported is required when HTTP/2 is disabled.")
        .required(true)
        .defaultValue("False")
        .allowableValues("True", "False")
        .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("SSL Context Service provides trusted certificates and client certificates for TLS communication.")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    public static final PropertyDescriptor MAX_RETRIES = new PropertyDescriptor.Builder()
        .name("max-retries")
        .displayName("Maximum Retries")
        .description("Maximum number of times each request will be retried if a 5xx status code is received or the request"
                + " times out. Set to 0 for no retries. Note that the retries are in addition to the initial request, so"
                + " 3 retries will mean up to 4 requests in total.")
        .required(true)
        .defaultValue("3")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor SOCKET_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Connection Timeout")
        .displayName("Socket Connect Timeout")
        .description("Maximum time to wait for initial socket connection to the HTTP URL.")
        .required(true)
        .defaultValue("5 secs")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();

    public static final PropertyDescriptor SOCKET_READ_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Read Timeout")
        .displayName("Socket Read Timeout")
        .description("Maximum time to wait for receiving responses from a socket connection to the HTTP URL.")
        .required(true)
        .defaultValue("15 secs")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();

    public static final PropertyDescriptor SOCKET_IDLE_TIMEOUT = new PropertyDescriptor.Builder()
        .name("idle-timeout")
        .displayName("Socket Idle Timeout")
        .description("Maximum time to wait before closing idle connections to the HTTP URL.")
        .required(true)
        .defaultValue("5 mins")
        .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.MILLISECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
        .build();

    public static final PropertyDescriptor SOCKET_IDLE_CONNECTIONS = new PropertyDescriptor.Builder()
        .name("max-idle-connections")
        .displayName("Socket Idle Connections")
        .description("Maximum number of idle connections to the HTTP URL.")
        .required(true)
        .defaultValue("5")
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
        .name("oauth2-access-token-provider")
        .displayName("Request OAuth2 Access Token Provider")
        .description("Enables managed retrieval of OAuth2 Bearer Token applied to HTTP requests using the Authorization Header.")
        .identifiesControllerService(OAuth2AccessTokenProvider.class)
        .required(false)
        .build();

    public static final PropertyDescriptor REQUEST_USERNAME = new PropertyDescriptor.Builder()
        .name("Basic Authentication Username")
        .displayName("Request Username")
        .description("The username provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
        .build();

    public static final PropertyDescriptor REQUEST_PASSWORD = new PropertyDescriptor.Builder()
        .name("Basic Authentication Password")
        .displayName("Request Password")
        .description("The password provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
        .build();

    public static final PropertyDescriptor REQUEST_DIGEST_AUTHENTICATION_ENABLED = new PropertyDescriptor.Builder()
        .name("Digest Authentication")
        .displayName("Request Digest Authentication Enabled")
        .description("Enable Digest Authentication on HTTP requests with Username and Password credentials as described in RFC 7616.")
        .required(false)
        .defaultValue("false")
        .allowableValues("true", "false")
        .dependsOn(REQUEST_USERNAME)
        .build();

    public static final PropertyDescriptor REQUEST_FORM_DATA_NAME = new PropertyDescriptor.Builder()
        .name("form-body-form-name")
        .displayName("Request Multipart Form-Data Name")
        .description("Enable sending HTTP request body formatted using multipart/form-data and using the form name configured.")
        .required(false)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
        .build();


    public static final PropertyDescriptor REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED = new PropertyDescriptor.Builder()
        .name("Use Chunked Encoding")
        .displayName("Request Chunked Transfer-Encoding Enabled")
        .description("Enable sending HTTP requests with the Transfer-Encoding Header set to chunked, and disable sending the Content-Length Header. " +
                "Transfer-Encoding applies to the body in HTTP/1.1 requests as described in RFC 7230 Section 3.3.1")
        .required(true)
        .defaultValue(Boolean.FALSE.toString())
        .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
        .build();

    public static final PropertyDescriptor REQUEST_CONTENT_ENCODING = new PropertyDescriptor.Builder()
        .name("Content-Encoding")
        .displayName("Request Content-Encoding")
        .description("HTTP Content-Encoding applied to request body during transmission. The receiving server must support the selected encoding to avoid request failures.")
        .required(true)
        .defaultValue(ContentEncodingStrategy.DISABLED.getValue())
        .allowableValues(ContentEncodingStrategy.class)
        .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
        .build();

    public static final PropertyDescriptor REQUEST_CONTENT_TYPE = new PropertyDescriptor.Builder()
        .name("Content-Type")
        .displayName("Request Content-Type")
        .description("HTTP Content-Type Header applied to when sending an HTTP request body for PATCH, POST, or PUT methods. " +
                String.format("The Content-Type defaults to %s when not configured.", DEFAULT_CONTENT_TYPE))
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(DEFAULT_CONTENT_TYPE)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
        .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
        .build();

    public static final PropertyDescriptor REQUEST_DATE_HEADER_ENABLED = new PropertyDescriptor.Builder()
        .name("Include Date Header")
        .displayName("Request Date Header Enabled")
        .description("Enable sending HTTP Date Header on HTTP requests as described in RFC 7231 Section 7.1.1.2.")
        .required(true)
        .defaultValue("True")
        .allowableValues("True", "False")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    public static final PropertyDescriptor REQUEST_USER_AGENT = new PropertyDescriptor.Builder()
        .name("Useragent")
        .displayName("Request User-Agent")
        .description("HTTP User-Agent Header applied to requests. RFC 7231 Section 5.5.3 describes recommend formatting.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor RESPONSE_STATUS_CODE_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-status-code-record-path")
        .displayName("Response Status Code RecordPath")
        .description("RecordPath of the field to write the HTTP response status code to (200, 404, etc). If left empty, "
                + "the status code will not be written to Records.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor RESPONSE_STATUS_TEXT_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-status-text-record-path")
        .displayName("Response Status Text RecordPath")
        .description("RecordPath of the field to write the HTTP response status text to (200, 404, etc). If left empty, "
                + "the status text will not be written to Records.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor RESPONSE_HEADERS_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-headers-record-path")
        .displayName("Response Headers RecordPath")
        .description("RecordPath of the field to write the HTTP response headers to. Headers will be written as a Map."
                + " If left empty, the headers will not be written to Records.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor RESPONSE_TIME_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-time-record-path")
        .displayName("Response Time RecordPath")
        .description("RecordPath of the field to write the time taken to process the HTT request to."
                + " If left empty, the processing time will not be written to Records.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_HANDLING = new PropertyDescriptor.Builder()
        .name("response-body-handling")
        .displayName("Handle Response Body As")
        .description("Whether to ignore the response body, treat it as a raw string or whether to parse it into a Record. Parsing into a Record "
                + "requires providing a Response Body Record Reader value, but also allows success or failure to be determined based on the "
                + "contents of the response body.")
        .required(true)
        .defaultValue(BODY_HANDLING_NONE.getValue())
        .allowableValues(BODY_HANDLING_NONE, BODY_HANDLING_STRING, BODY_HANDLING_RECORD)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-body-record-path")
        .displayName("Response Body RecordPath")
        .description("RecordPath of the field to write the response body to. If left empty, the response body will not be written to Records.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(RESPONSE_BODY_HANDLING, BODY_HANDLING_STRING, BODY_HANDLING_RECORD)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_STRING_MAX_BYTES = new PropertyDescriptor.Builder()
        .name("response-body-max-bytes")
        .displayName("Response Body String Maximum Bytes")
        .description("Maximum size in bytes (not UTF-8 characters) applied when writing an HTTP response body to a string Record field. "
                + "Responses exceeding the maximum byte length will be truncated. Set to 0 to not truncate response bodies.")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("0")
        .dependsOn(RESPONSE_BODY_HANDLING, BODY_HANDLING_STRING)
        .build();

    static final PropertyDescriptor RESPONSE_BODY_RECORD_READER = new PropertyDescriptor.Builder()
        .name("response-body-record-reader")
        .displayName("Response Body Record Reader")
        .description("Specifies the Controller Service to use for reading request body data, which is then used to determine whether a request has"
                + " succeeded or failed based on applying a RecordPath to the response body content.")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .dependsOn(RESPONSE_BODY_HANDLING, BODY_HANDLING_RECORD)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_SUCCESS_CRITERIA_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("response-body-success-criteria-record-path")
        .displayName("Response Body Success Criteria RecordPath")
        .description("This RecordPath is applied to the request body Record and the result is compared to a fixed value to determine"
                + " whether the request is assumed to be successful or to have failed (and will be retried). Leave empty to not"
                + " determine success or failure based on the response body.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(RESPONSE_BODY_RECORD_READER)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_SUCCESS_CRITERIA_OPERATOR = new PropertyDescriptor.Builder()
        .name("response-body-success-criteria-operator")
        .displayName("Response Body Success Criteria Comparison Operator")
        .description("Operator used to compare the success criteria RecordPath result to the comparison value.")
        .allowableValues(
                SUCCESS_CRITERIA_EMPTY,
                SUCCESS_CRITERIA_NOT_EMPTY,
                SUCCESS_CRITERIA_LESS_THAN,
                SUCCESS_CRITERIA_LESS_THAN_OR_EQUAL,
                SUCCESS_CRITERIA_EQUAL,
                SUCCESS_CRITERIA_NOT_EQUAL,
                SUCCESS_CRITERIA_GREATER_THAN_OR_EQUAL,
                SUCCESS_CRITERIA_GREATER_THAN)
        .defaultValue(SUCCESS_CRITERIA_EQUAL.getValue())
        .required(true)
        .dependsOn(RESPONSE_BODY_SUCCESS_CRITERIA_RECORD_PATH)
        .build();

    public static final PropertyDescriptor RESPONSE_BODY_SUCCESS_CRITERIA_VALUE = new PropertyDescriptor.Builder()
        .name("response-body-success-criteria-value")
        .displayName("Response Body Success Criteria Comparison Value")
        .description("Value the Success Criteria RecordPath result is compared to.")
        .required(false)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(
                RESPONSE_BODY_SUCCESS_CRITERIA_OPERATOR,
                SUCCESS_CRITERIA_LESS_THAN,
                SUCCESS_CRITERIA_LESS_THAN_OR_EQUAL,
                SUCCESS_CRITERIA_EQUAL,
                SUCCESS_CRITERIA_NOT_EQUAL,
                SUCCESS_CRITERIA_GREATER_THAN_OR_EQUAL,
                SUCCESS_CRITERIA_GREATER_THAN)
        .build();

    public static final PropertyDescriptor RESPONSE_CACHE_ENABLED = new PropertyDescriptor.Builder()
        .name("use-etag")
        .displayName("Response Cache Enabled")
        .description("Enable HTTP response caching described in RFC 7234. Caching responses considers ETag and other headers.")
        .required(true)
        .defaultValue(Boolean.FALSE.toString())
        .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .build();

    public static final PropertyDescriptor RESPONSE_CACHE_SIZE = new PropertyDescriptor.Builder()
        .name("etag-max-cache-size")
        .displayName("Response Cache Size")
        .description("Maximum size of HTTP response cache in bytes. Caching responses considers ETag and other headers.")
        .required(true)
        .defaultValue("10MB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .dependsOn(RESPONSE_CACHE_ENABLED, Boolean.TRUE.toString())
        .build();

    public static final PropertyDescriptor RESPONSE_COOKIE_STRATEGY = new PropertyDescriptor.Builder()
        .name("cookie-strategy")
        .description("Strategy for accepting and persisting HTTP cookies. Accepting cookies enables persistence across multiple requests.")
        .displayName("Response Cookie Strategy")
        .required(true)
        .defaultValue(CookieStrategy.DISABLED.name())
        .allowableValues(CookieStrategy.values())
        .build();

    public static final PropertyDescriptor RESPONSE_REDIRECTS_ENABLED = new PropertyDescriptor.Builder()
        .name("Follow Redirects")
        .displayName("Response Redirects Enabled")
        .description("Enable following HTTP redirects sent with HTTP 300 series responses as described in RFC 7231 Section 6.4.")
        .required(true)
        .defaultValue("True")
        .allowableValues("True", "False")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final PropertyDescriptor HTTP_HEADER_TYPE = new PropertyDescriptor.Builder()
        .name("header-type")
        .displayName("HTTP Header Type")
        .description("Whether dynamic property values are interpreted as literal strings or as RecordPaths.")
        .allowableValues(HEADER_TYPE_LITERAL, HEADER_TYPE_RECORD_PATH)
        .defaultValue(HEADER_TYPE_LITERAL.getValue())
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};

    private static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
        RECORD_READER,
        RECORD_WRITER,
        ERROR_ROUTING,
        HTTP_METHOD,
        HTTP_URL_TYPE,
        HTTP_URL_LITERAL,
        HTTP_URL_RECORD_PATH,
        REQUEST_BODY_TYPE,
        REQUEST_BODY_LITERAL,
        REQUEST_BODY_RECORD_PATH,
        HTTP2_DISABLED,
        SSL_CONTEXT_SERVICE,
        MAX_RETRIES,
        SOCKET_CONNECT_TIMEOUT,
        SOCKET_READ_TIMEOUT,
        SOCKET_IDLE_TIMEOUT,
        SOCKET_IDLE_CONNECTIONS,
        PROXY_CONFIGURATION_SERVICE,
        REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER,
        REQUEST_USERNAME,
        REQUEST_PASSWORD,
        REQUEST_DIGEST_AUTHENTICATION_ENABLED,
        REQUEST_FORM_DATA_NAME,
        REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED,
        REQUEST_CONTENT_ENCODING,
        REQUEST_CONTENT_TYPE,
        REQUEST_DATE_HEADER_ENABLED,
        REQUEST_USER_AGENT,
        RESPONSE_STATUS_CODE_RECORD_PATH,
        RESPONSE_STATUS_TEXT_RECORD_PATH,
        RESPONSE_HEADERS_RECORD_PATH,
        RESPONSE_TIME_RECORD_PATH,
        RESPONSE_BODY_HANDLING,
        RESPONSE_BODY_RECORD_PATH,
        RESPONSE_BODY_STRING_MAX_BYTES,
        RESPONSE_BODY_RECORD_READER,
        RESPONSE_BODY_SUCCESS_CRITERIA_RECORD_PATH,
        RESPONSE_BODY_SUCCESS_CRITERIA_OPERATOR,
        RESPONSE_BODY_SUCCESS_CRITERIA_VALUE,
        RESPONSE_CACHE_ENABLED,
        RESPONSE_CACHE_SIZE,
        RESPONSE_COOKIE_STRATEGY,
        RESPONSE_REDIRECTS_ENABLED,
        HTTP_HEADER_TYPE
    ));

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original flowfile will be routed to this relationship")
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The flowfile augmented with successful request data will be routed to this relationship")
            .build();
    static final Relationship REL_SPLIT_SUCCESS = new Relationship.Builder()
            .name("split-success")
            .description("If an HTTP request is successfully made for a record, it will be routed to this relationship")
            .build();
    static final Relationship REL_SPLIT_FAILURE = new Relationship.Builder()
            .name("split-failure")
            .description("If an HTTP request fails for a record, either the failing record(s) or the entire flowfile"
                    + "(depending on the Error Routing setting) will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original flowfile will be routed to this relationship if an unrecoverable error is encountered")
            .build();

    private volatile Set<Relationship> relationships = new HashSet<>(Arrays.asList(REL_ORIGINAL, REL_FAILURE));

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context) throws TlsException, IOException {
        // TODO: Need to set? https://square.github.io/okhttp/3.x/okhttp/okhttp3/OkHttpClient.Builder.html#retryOnConnectionFailure-boolean-

        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        final Proxy proxy = proxyConfig.createProxy();
        if (!Type.DIRECT.equals(proxy.type())) {
            okHttpClientBuilder.proxy(proxy);
            if (proxyConfig.hasCredential()) {
                ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
            }
        }

        // Configure caching
        final boolean cachingEnabled = context.getProperty(RESPONSE_CACHE_ENABLED).asBoolean();
        if (cachingEnabled) {
            final int maxCacheSizeBytes = context.getProperty(RESPONSE_CACHE_SIZE).asDataSize(DataUnit.B).intValue();
            okHttpClientBuilder.cache(new Cache(getResponseCacheDirectory(), maxCacheSizeBytes));
        }

        if (context.getProperty(HTTP2_DISABLED).asBoolean()) {
            okHttpClientBuilder.protocols(Collections.singletonList(Protocol.HTTP_1_1));
        }

        okHttpClientBuilder.followRedirects(context.getProperty(RESPONSE_REDIRECTS_ENABLED).asBoolean());
        okHttpClientBuilder.connectTimeout((context.getProperty(SOCKET_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(context.getProperty(SOCKET_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.connectionPool(
                new ConnectionPool(
                        context.getProperty(SOCKET_IDLE_CONNECTIONS).asInteger(),
                        context.getProperty(SOCKET_IDLE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS
                )
        );

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslService != null) {
            final SSLContext sslContext = sslService.createContext();
            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            final TlsConfiguration tlsConfiguration = sslService.createTlsConfiguration();
            final X509TrustManager trustManager = Objects.requireNonNull(SslContextFactory.getX509TrustManager(tlsConfiguration), "Trust Manager not found");
            okHttpClientBuilder.sslSocketFactory(socketFactory, trustManager);
        }

        final CookieStrategy cookieStrategy = CookieStrategy.valueOf(context.getProperty(RESPONSE_COOKIE_STRATEGY).getValue());
        switch (cookieStrategy) {
            case DISABLED:
                break;
            case ACCEPT_ALL:
                final CookieManager cookieManager = new CookieManager();
                cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
                okHttpClientBuilder.cookieJar(new JavaNetCookieJar(cookieManager));
                break;
        }

        setAuthenticator(okHttpClientBuilder, context);

        chunkedTransferEncoding = context.getProperty(REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED).asBoolean();

        okHttpClientAtomicReference.set(okHttpClientBuilder.build());
    }

    @OnScheduled
    public void initOauth2AccessTokenProvider(final ProcessContext context) {
        if (context.getProperty(REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context.getProperty(REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

            oauth2AccessTokenProvider.getAccessDetails();

            oauth2AccessTokenProviderOptional = Optional.of(oauth2AccessTokenProvider);
        } else {
            oauth2AccessTokenProviderOptional = Optional.empty();
        }
    }

    private void setAuthenticator(OkHttpClient.Builder okHttpClientBuilder, ProcessContext context) {
        final String authUser = trimToEmpty(context.getProperty(REQUEST_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && "true".equalsIgnoreCase(context.getProperty(REQUEST_DIGEST_AUTHENTICATION_ENABLED).getValue())) {
            final String authPass = trimToEmpty(context.getProperty(REQUEST_PASSWORD).getValue());

            final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(authUser, authPass);
            final DigestAuthenticator digestAuthenticator = new DigestAuthenticator(credentials);

            okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
            okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.addAll(PROPERTIES);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .description("Property name will be used as the header name. Property value will either be used as the"
                        + " literal header value or a RecordPath used to fetch the header value, depending on the HTTP Header Type property.")
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @OnScheduled
    public void createRecordPathCache(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (ERROR_ROUTING.equals(descriptor)) {
            if (ERROR_SPLIT_FAILURES.getValue().equalsIgnoreCase(newValue)) {
                final Set<Relationship> newRelationships = new HashSet<>();
                newRelationships.add(REL_ORIGINAL);
                newRelationships.add(REL_SPLIT_SUCCESS);
                newRelationships.add(REL_SPLIT_FAILURE);
                newRelationships.add(REL_FAILURE);

                this.relationships = newRelationships;
            } else {
                final Set<Relationship> newRelationships = new HashSet<>();
                newRelationships.add(REL_ORIGINAL);
                newRelationships.add(REL_SUCCESS);
                newRelationships.add(REL_FAILURE);

                this.relationships = newRelationships;
            }
        } else if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final boolean splitErrors = context.getProperty(ERROR_ROUTING).getValue().equals(ERROR_SPLIT_FAILURES.getValue());
        logger.debug("splitErrors: " + splitErrors);

        final FlowFile successFlowFile = session.clone(originalFlowFile);
        final WriteResult successWriteResult;

        FlowFile failureFlowFile = null;
        WriteResult failureWriteResult = null;

        try {
            RecordSetWriter writerSuccess = null;
            RecordSetWriter writerFailure = null;

            try (
                final InputStream in = session.read(originalFlowFile);
            ) {
                final Map<String, String> originalAttributes = originalFlowFile.getAttributes();
                final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, originalFlowFile.getSize(), getLogger());

                final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                final OutputStream outSuccess = session.write(successFlowFile);
                writerSuccess = writerFactory.createWriter(getLogger(), writeSchema, outSuccess, originalFlowFile);
                writerSuccess.beginRecordSet();

                Record record;
                int count = 0;
                while ((record = reader.nextRecord()) != null) {
                    boolean success = processRecord(record, originalFlowFile, context, count);
                    count++;

                    // If the request succeeded (after any retries), write it to the success RecordSet.
                    if (success) {
                        logger.debug("Writing record to success RecordSet");
                        writerSuccess.write(record);
                    } else if (splitErrors) {
                        // If we're splitting records with errors into a separate flowfile, first check that the failure
                        // flowfile and writer exist, and create them if they don't.
                        if (writerFailure == null) {
                            logger.debug("Creating failure flowfile and RecordSet");
                            failureFlowFile = session.create(originalFlowFile);
                            final OutputStream outFailure = session.write(failureFlowFile);
                            writerFailure = writerFactory.createWriter(getLogger(), writeSchema, outFailure, failureFlowFile);
                            writerFailure.beginRecordSet();
                        }

                        // Write the failed record to the failure RecordSet.
                        logger.debug("Writing record to failure RecordSet");
                        writerFailure.write(record);
                    } else {
                        // Otherwise just throw an exception, which will force the whole input flowfile to be routed to
                        // the failure relationship.
                        logger.debug("HTTP request failed and not splitting records, aborting");
                        throw new Exception("HTTP request failed and not splitting records");
                    }
                }

                // Finish the record sets
                successWriteResult = writerSuccess.finishRecordSet();

                if (writerFailure != null) {
                    failureWriteResult = writerFailure.finishRecordSet();
                }
            } finally {
                try {
                    if (writerSuccess != null) {
                        writerSuccess.close();
                    }
                } catch (Exception e) {
                    logger.debug("Error closing success writer: ", e);
                }

                try {
                    if (writerFailure != null) {
                        writerFailure.close();
                    }
                } catch (Exception e) {
                    logger.debug("Error closing failure writer: ", e);
                }
            }

            // Number of split flowfiles we output. Will always be either 1 for just success or 2 for both success and failure.
            final int fragmentCount = writerFailure == null ? 1 : 2;

            // Generate a random ID to set on split output flowfiles.
            final String fragmentId = UUID.randomUUID().toString();

            // Index on each output flowfile produced. Will only ever be 1 or 2 for this processor, as we only produce
            // up to two output flowfiles.
            int fragmentIndex = 1;

            if (splitErrors) {
                final Map<String, String> successAttributes = new HashMap<>();
                successAttributes.put("record.count", String.valueOf(successWriteResult.getRecordCount()));
                successAttributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), String.valueOf(fragmentCount));
                successAttributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                successAttributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex));
                session.putAllAttributes(successFlowFile, successAttributes);

                logger.debug("Transferring updated FlowFile to split-success relationship");
                session.transfer(successFlowFile, REL_SPLIT_SUCCESS);
            } else {
                logger.debug("Transferring updated FlowFile to success relationship");
                session.transfer(successFlowFile, REL_SUCCESS);
            }

            // If we have any request failures, write them to a separate flowfile. (If we're not in split failures
            // mode and we encounter a request error, we'll never reach this point.)
            if (failureWriteResult != null) {
                fragmentIndex++;
                final Map<String, String> failureAttributes = new HashMap<>();
                failureAttributes.put("record.count", String.valueOf(failureWriteResult.getRecordCount()));
                failureAttributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), String.valueOf(fragmentCount));
                failureAttributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                failureAttributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex));
                session.putAllAttributes(failureFlowFile, failureAttributes);

                logger.debug("Transferring failure flowfile to split-failure relationship");
                session.transfer(failureFlowFile, REL_SPLIT_FAILURE);
            }

            logger.debug("Transferring original FlowFile to original relationship");
            session.transfer(originalFlowFile, REL_ORIGINAL);
        } catch (final Exception e) {
            logger.error("Error processing flowfile:", e);

            // Remove the separate success and failure flowfiles, if they've been created.
            logger.debug("Removing success flowfile");
            session.remove(successFlowFile);

            if (failureFlowFile != null) {
                logger.debug("Removing failure flowfile");
                session.remove(failureFlowFile);
            }

            // Route the original flowfile to failure. This can occur either when there's an unrecoverable error, or if
            // an HTTP request fails and we're not in "split errors" mode.
            logger.debug("Transferring original FlowFile to failure relationship");
            session.transfer(originalFlowFile, REL_FAILURE);
        }
    }

    /**
     * Send an HTTP request and update a record, optionally retrying on server errors or timeouts.
     */
    private boolean processRecord(Record record, final FlowFile originalFlowFile, final ProcessContext context, int count) {
        final ComponentLog logger = getLogger();
        final int maxRetries = context.getProperty(MAX_RETRIES).asInteger();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                logger.debug("[record " + count + "] Retry attempt " + attempt);
            } else {
                logger.debug("[record " + count + "] Sending HTTP request...");
            }

            HttpRequestResult result = sendRequestAndUpdateRecord(record, originalFlowFile, context);
            logger.debug("[record " + count + "] Result: " + result);

            // If we got an HTTP server error, the request timed out or there was a response body error, retry.
            if (result == HttpRequestResult.HTTP_SERVER_ERROR
                    || result == HttpRequestResult.TIMEOUT
                    || result == HttpRequestResult.RESPONSE_BODY_CRITERIA_FAILED
            ) {
                logger.debug("[record " + count + "] Server error, timeout or response body error");
                continue;
            } else if (result == HttpRequestResult.HTTP_SUCCESS) {
                // Otherwise if the result was a success, return true.
                logger.debug("[record " + count + "] HTTP request successful");
                return true;
            } else {
                // Otherwise if we got any other result (an internal error in this processor, an HTTP client error,
                // etc), return false.
                return false;
            }
        }

        // If we run out of retries, give up and return false.
        logger.debug("[record " + count + "] Request failed after " + maxRetries + " retries, aborting");
        return false;
    }

    /**
     * Sends an HTTP request for a Record, writes the results to it and returns a result value.
     */
    private HttpRequestResult sendRequestAndUpdateRecord(Record record, final FlowFile originalFlowFile, final ProcessContext context) {
        final ComponentLog logger = getLogger();

        final String requestUrl;
        final String urlType = context.getProperty(HTTP_URL_TYPE).getValue();
        if (urlType.equals(URL_TYPE_LITERAL.getValue())) {
            logger.debug("In literal URL mode");
            requestUrl = context.getProperty(HTTP_URL_LITERAL).evaluateAttributeExpressions().getValue();
        } else if (urlType.equals(URL_TYPE_RECORD_PATH.getValue())) {
            logger.debug("In RecordPath URL mode");
            final RecordPath recordPath = recordPathCache.getCompiled(context.getProperty(HTTP_URL_RECORD_PATH).evaluateAttributeExpressions().getValue());
            logger.debug("Found record path: " + recordPath);
            final Optional<FieldValue> value = recordPath.evaluate(record).getSelectedFields().findFirst();

            if (value.isEmpty()) {
                final String message = "Record path \"" + recordPath + "\" did not resolve to a value for URL, skipping record";
                logger.error(message);

                setRecordFieldByProperty(record, RESPONSE_STATUS_CODE_RECORD_PATH, "0", context);
                setRecordFieldByProperty(record, RESPONSE_STATUS_TEXT_RECORD_PATH, message, context);
                return HttpRequestResult.FAILURE;
            }

            requestUrl = value.get().getValue().toString();
        } else {
            final String message = "Unknown URL type \"" + urlType + "\", skipping record";
            logger.error(message);

            setRecordFieldByProperty(record, RESPONSE_STATUS_CODE_RECORD_PATH, "0", context);
            setRecordFieldByProperty(record, RESPONSE_STATUS_TEXT_RECORD_PATH, message, context);
            return HttpRequestResult.FAILURE;
        }

        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        try {
            final URL url = new URL(requestUrl);
            logger.debug("Request URL: " + url);

            Request httpRequest = configureRequest(record, context, originalFlowFile, url);
            logRequest(logger, httpRequest);

            final long startNanos = System.nanoTime();
            logger.debug("Sending HTTP request...");
            int statusCode;
            boolean responseBodySuccess = true;
            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
                logResponse(logger, url, responseHttp);

                // Store the status code and message
                statusCode = responseHttp.code();
                String statusMessage = responseHttp.message();
                logger.debug("Response status code: " + statusCode);

                // Set some record fields with response metadata.
                setRecordFieldByProperty(record, RESPONSE_STATUS_CODE_RECORD_PATH, String.valueOf(statusCode), context);
                setRecordFieldByProperty(record, RESPONSE_STATUS_TEXT_RECORD_PATH, statusMessage, context);
                setRecordFieldByProperty(record, RESPONSE_TIME_RECORD_PATH, Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)), context);
                setRecordFieldByProperty(record, RESPONSE_HEADERS_RECORD_PATH, convertAttributesFromHeaders(responseHttp), context);

                ResponseBody responseBody = responseHttp.body();
                boolean bodyExists = responseBody != null;
                logger.debug("bodyExists: " + bodyExists);

                final String responseBodyHandling = context.getProperty(RESPONSE_BODY_HANDLING).getValue();
                if (responseBodyHandling.equals(BODY_HANDLING_NONE.getValue())) {
                    logger.debug("Response body handling configured to ignore body, assuming success");
                } else if (responseBodyHandling.equals(BODY_HANDLING_STRING.getValue())) {
                    logger.debug("Handling response body as string, assuming success");
                    handleResponseBodyAsString(record, responseBody, context);
                } else if (responseBodyHandling.equals(BODY_HANDLING_RECORD.getValue())) {
                    logger.debug("Handling response body as Record, checking success criteria");
                    responseBodySuccess = handleResponseBodyAsRecord(record, responseBody, originalFlowFile, context);
                    logger.debug("Response body success criteria " + (responseBodySuccess ? "met" : "not met"));
                } else {
                    logger.error("Unknown response body handling mode, assuming failure: " + responseBodyHandling);
                    responseBodySuccess = false;
                }
            }

            record.incorporateInactiveFields();

            if (statusCode >= 400 && statusCode <= 499) {
                return HttpRequestResult.HTTP_CLIENT_ERROR;
            } else if (statusCode >= 500 && statusCode <= 599) {
                return HttpRequestResult.HTTP_SERVER_ERROR;
            } else if (!responseBodySuccess) {
                return HttpRequestResult.RESPONSE_BODY_CRITERIA_FAILED;
            } else if (statusCode >= 200 && statusCode <= 299) {
                return HttpRequestResult.HTTP_SUCCESS;
            } else {
                return HttpRequestResult.HTTP_OTHER;
            }
        } catch (final SocketTimeoutException e) {
            logger.error("Request timed out", e);

            try {
                setRecordFieldByProperty(record, RESPONSE_STATUS_CODE_RECORD_PATH, "0", context);
                setRecordFieldByProperty(record, RESPONSE_STATUS_TEXT_RECORD_PATH, "Request timed out: " + e.getMessage(), context);
            } catch (Exception err) {
                logger.warn("Error writing timeout details to RecordPaths", err);
            }

            return HttpRequestResult.TIMEOUT;
        } catch (final Exception e) {
            logger.error("Updating record failed", e);

            try {
                setRecordFieldByProperty(record, RESPONSE_STATUS_CODE_RECORD_PATH, "0", context);
                setRecordFieldByProperty(record, RESPONSE_STATUS_TEXT_RECORD_PATH, "Updating record failed:" + e.getMessage(), context);
            } catch (Exception err) {
                logger.warn("Error writing error details to RecordPaths", err);
            }

            return HttpRequestResult.FAILURE;
        }
    }

    /**
     * Stores the response body on a Record, if configured, and optionally returns a success or failure result based on
     * the response body.
     */
    private void handleResponseBodyAsString(Record record, final ResponseBody responseBody, final ProcessContext context) throws IOException {
        final ComponentLog logger = getLogger();

        final String responseBodyPath = context.getProperty(RESPONSE_BODY_RECORD_PATH).getValue();
        if (responseBodyPath == null || responseBodyPath.isBlank()) {
            logger.debug("No response body storage record path defined, not storing response");
            return;
        }

        // Parse the response into a string.
        final String bodyString;
        try (InputStream responseBodyStream = responseBody.byteStream()) {
            ByteArrayOutputStream bodyStringStream = new ByteArrayOutputStream();

            final int maxResponseBodyBytes = context.getProperty(RESPONSE_BODY_STRING_MAX_BYTES).asInteger();
            long bytesCopied;
            if (maxResponseBodyBytes > 0) {
                logger.debug("Only copying up to " + maxResponseBodyBytes + " bytes of response body");
                bytesCopied = copyStreamWithMaximum(responseBodyStream, bodyStringStream, maxResponseBodyBytes);
            } else {
                logger.debug("maxResponseBodyBytes is 0, not limiting response body size");
                bytesCopied = StreamUtils.copy(responseBodyStream, bodyStringStream);
            }

            logger.debug("Copied " + bytesCopied + " response body bytes");
            bodyString = bodyStringStream.toString(StandardCharsets.UTF_8);
        }

        logger.debug("Storing response body as string at RecordPath: " + responseBodyPath);
        setRecordFieldByProperty(record, RESPONSE_BODY_RECORD_PATH, bodyString, context);
    }

    /**
     * Copies bytes from the input stream to the output stream, up to a maximum number of bytes.
     */
    private long copyStreamWithMaximum(final InputStream source, final OutputStream destination, final long maxBytes) throws IOException {
        final byte[] buffer = new byte[8192];
        int len;
        long totalCount = 0L;
        while ((len = source.read(buffer)) > 0) {
            if (totalCount + len < maxBytes) {
                destination.write(buffer, 0, len);
            } else {
                destination.write(buffer, 0, (int) (maxBytes - totalCount));
            }
            totalCount += len;
        }

        return totalCount;
    }

    /**
     * Stores the response body on a Record, if configured, and optionally returns a success or failure result based on
     * the response body.
     */
    private boolean handleResponseBodyAsRecord(
            final Record record,
            final ResponseBody responseBody,
            final FlowFile originalFlowFile,
            final ProcessContext context
    ) {
        final ComponentLog logger = getLogger();

        // If we don't have a response body reader set, just return true as we're not checking the body.
        final PropertyValue bodyReaderFactoryProp = context.getProperty(RESPONSE_BODY_RECORD_READER);
        if (bodyReaderFactoryProp.getValue() == null) {
            logger.error("Treating response body as record but no response body reader set");
            return false;
        }

        // Use a RecordReader to parse the response body as a Record.
        final Record bodyRecord;
        try (BufferedInputStream responseBodyStream = new BufferedInputStream(responseBody.byteStream())) {
            final RecordReaderFactory bodyReaderFactory = context.getProperty(RESPONSE_BODY_RECORD_READER).asControllerService(RecordReaderFactory.class);
            if (bodyReaderFactory == null) {
                logger.error("Could not load record reader " + bodyReaderFactoryProp.getValue() + ", assuming failure");
                return false;
            }

            final Map<String, String> originalAttributes = originalFlowFile.getAttributes();
            final RecordReader reader = bodyReaderFactory.createRecordReader(originalAttributes, responseBodyStream, responseBody.contentLength(), logger);
            bodyRecord = reader.nextRecord();
            logger.debug("Successfully parsed response body into record");

            reader.close();
        } catch (Exception e) {
            logger.error("Error parsing response body into record, assuming failure: " + e);
            return false;
        }

        final String responseBodyPath = context.getProperty(RESPONSE_BODY_RECORD_PATH).getValue();
        if (responseBodyPath != null && !responseBodyPath.isBlank()) {
            logger.debug("Storing response body at RecordPath: " + responseBodyPath);
            mergeRecordFieldByPath(record, responseBodyPath, bodyRecord);
        } else {
            logger.debug("No response body storage record path defined, not storing response");
        }

        final String recordPathString = context.getProperty(RESPONSE_BODY_SUCCESS_CRITERIA_RECORD_PATH).getValue();
        if (recordPathString == null || recordPathString.isBlank()) {
            logger.debug("Missing or blank response body success criteria record path, not evaluating response body success criteria");
            return true;
        }

        // If we have a body string, attempt to check if it signals a failed request requiring a retry.
        return checkResponseBodySuccessCriteria(bodyRecord, context);
    }

    /**
     * If response body success criteria parameters have been set, extract a value from the response body, compare it
     * to a configured value using a configured parameter and return the result. This can be used to treat responses as
     * failing based on the body content (e.g. a JSON "error" field), even though they return an HTTP 200 status code.
     */
    private boolean checkResponseBodySuccessCriteria(final Record record, final ProcessContext context) {
        final ComponentLog logger = getLogger();

        try {
            final String recordPathString = context.getProperty(RESPONSE_BODY_SUCCESS_CRITERIA_RECORD_PATH).getValue();
            if (recordPathString == null || recordPathString.isBlank()) {
                logger.debug("Missing or blank response body success criteria record path, not evaluating response body success criteria");
                return true;
            }

            final RecordPath recordPath = recordPathCache.getCompiled(recordPathString);
            Optional<FieldValue> result = recordPath.evaluate(record).getSelectedFields().findFirst();
            logger.debug("Response body success criteria RecordPath result: " + result);

            final String fieldStringValue;
            if (result.isPresent()) {
                Object fieldValue = result.get().getValue();
                if (fieldValue == null) {
                    logger.debug("Record field value is null, assuming empty string");
                    fieldStringValue = "";
                } else {
                    fieldStringValue = fieldValue.toString();
                }
            } else {
                logger.debug("Could not find record field, assuming empty string");
                fieldStringValue = "";
            }

            logger.debug("Response body success criteria RecordPath result as string: " + fieldStringValue);

            final String operator = context.getProperty(RESPONSE_BODY_SUCCESS_CRITERIA_OPERATOR).getValue();
            final String comparisonValue = context.getProperty(RESPONSE_BODY_SUCCESS_CRITERIA_VALUE).getValue();
            final boolean comparisonResult = compareValues(fieldStringValue, operator, comparisonValue);
            logger.debug("Comparison result: \"" + fieldStringValue + "\" " + operator + " " + comparisonValue + " = " + comparisonResult);

            return comparisonResult;
        } catch (Exception e) {
            logger.error("Error evaluating response body success criteria, assuming failure", e);
            return false;
        }
    }

    /**
     * Compare two string values using one of the defined operators.
     */
    private boolean compareValues(String fieldValue, String operator, String comparisonValue) {
        switch (operator) {
            case OP_EMPTY:
                return fieldValue.isEmpty();

            case OP_NOT_EMPTY:
                return !fieldValue.isEmpty();

            case OP_LESS_THAN:
                return fieldValue.compareTo(comparisonValue) < 0;

            case OP_LESS_THAN_OR_EQUAL:
                return fieldValue.compareTo(comparisonValue) <= 0;

            case OP_EQUAL:
                return fieldValue.compareTo(comparisonValue) == 0;

            case OP_NOT_EQUAL:
                return fieldValue.compareTo(comparisonValue) != 0;

            case OP_GREATER_THAN_OR_EQUAL:
                return fieldValue.compareTo(comparisonValue) >= 0;

            case OP_GREATER_THAN:
                return fieldValue.compareTo(comparisonValue) > 0;

            default:
                throw new IllegalArgumentException("Unknown comparison operator: " + operator);
        }
    }

    /**
     * Fetches a RecordPath from a property and then uses it to set a Record field to a value.
     */
    private void setRecordFieldByProperty(final Record record, final PropertyDescriptor property, final Object value, final ProcessContext context) {
        final String path = context.getProperty(property).evaluateAttributeExpressions().getValue();
        if (path == null || path.isEmpty()) {
            getLogger().debug("RecordPath in property " + property.getName() + " null or empty, not setting a value");
            return;
        }

        setFieldByRecordPath(record, path, value);
    }

    /**
     * Sets a Record field (specified by a RecordPath) to a given value.
     */
    private void setFieldByRecordPath(final Record record, final String path, final Object value) {
        final RecordPath recordPath = recordPathCache.getCompiled(path);
        final Optional<FieldValue> fieldValue = recordPath.evaluate(record).getSelectedFields().findFirst();

        if (fieldValue.isEmpty()) {
            getLogger().error("RecordPath \"" + path + "\" did not resolve, not setting a value");
            return;
        }

        getLogger().debug("Setting RecordPath " + path + " to: " + value);
        fieldValue.get().updateValue(value);
    }

    /**
     * Merge a Record into a parent Record at the given RecordPath. If the RecordPath's target is a
     * Record, the child record's fields will be merged into it. Otherwise, the child record will
     * overwrite the target field.
     */
    private void mergeRecordFieldByPath(final Record parentRecord, final String path, final Record childRecord) {
        final RecordPath recordPath = recordPathCache.getCompiled(path);
        final Optional<FieldValue> targetField = recordPath.evaluate(parentRecord).getSelectedFields().findFirst();

        if (targetField.isEmpty()) {
            getLogger().error("RecordPath \"" + path + "\" did not resolve, not setting a value");
            return;
        }

        final Object destinationValue = targetField.get().getValue();
        if (destinationValue instanceof Record) {
            // If the destination field value is a Record, merge the child Record's fields into it.
            final Record destinationRecord = (Record) destinationValue;

            for (final String childRecordFieldName : childRecord.getRawFieldNames()) {
                final Object childRecordValue = childRecord.getValue(childRecordFieldName);

                final Optional<RecordField> childRecordFieldOptional = childRecord.getSchema().getField(childRecordFieldName);
                if (childRecordFieldOptional.isPresent()) {
                    RecordField childRecordField = childRecordFieldOptional.get();
                    if (!childRecordField.isNullable()) {
                        childRecordField = new RecordField(
                                childRecordField.getFieldName(),
                                childRecordField.getDataType(),
                                childRecordField.getDefaultValue(),
                                childRecordField.getAliases(),
                                true
                        );
                    }

                    destinationRecord.setValue(childRecordField, childRecordValue);
                } else {
                    destinationRecord.setValue(childRecordFieldName, childRecordValue);
                }
            }
        } else {
            final Optional<Record> parentOptional = targetField.get().getParentRecord();
            parentOptional.ifPresent(parent -> parent.setValue(targetField.get().getField(), childRecord));
        }
    }

    /**
     * Returns a Map of flowfile attributes from the response HTTP headers. Multivalue headers are naively converted to comma separated strings.
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

    private void logRequest(ComponentLog logger, Request request) {
        if (logger.isDebugEnabled()) {
            logger.debug("\nRequest to remote service:\n\t{}\n{}",
                    request.url().url().toExternalForm(), getLogString(request.headers().toMultimap()));
        }
    }

    private void logResponse(ComponentLog logger, URL url, Response response) {
        if (logger.isDebugEnabled()) {
            logger.debug("\nResponse from remote service:\n\t{}\n{}",
                    url.toExternalForm(), getLogString(response.headers().toMultimap()));
        }
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
                    sb.append(list);
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private Request configureRequest(final Record record, final ProcessContext context, final FlowFile requestFlowFile, URL url) throws Exception {
        final Request.Builder requestBuilder = new Request.Builder();

        requestBuilder.url(url);
        final String authUser = trimToEmpty(context.getProperty(REQUEST_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if ("false".equalsIgnoreCase(context.getProperty(REQUEST_DIGEST_AUTHENTICATION_ENABLED).getValue())) {
            if (!authUser.isEmpty()) {
                final String authPass = trimToEmpty(context.getProperty(REQUEST_PASSWORD).getValue());

                String credential = Credentials.basic(authUser, authPass);
                requestBuilder.header(HttpHeader.AUTHORIZATION.getHeader(), credential);
            } else {
                oauth2AccessTokenProviderOptional.ifPresent(oauth2AccessTokenProvider ->
                    requestBuilder.addHeader(HttpHeader.AUTHORIZATION.getHeader(), "Bearer " + oauth2AccessTokenProvider.getAccessDetails().getAccessToken())
                );
            }
        }

        final String contentEncoding = context.getProperty(REQUEST_CONTENT_ENCODING).getValue();
        final ContentEncodingStrategy contentEncodingStrategy = ContentEncodingStrategy.valueOf(contentEncoding);
        if (ContentEncodingStrategy.GZIP == contentEncodingStrategy) {
            requestBuilder.addHeader(HttpHeader.CONTENT_ENCODING.getHeader(), ContentEncodingStrategy.GZIP.getValue().toLowerCase());
        }

        final String method = getRequestMethod(context, requestFlowFile);
        final Optional<HttpMethod> httpMethodFound = findRequestMethod(method);

        final RequestBody requestBody;
        if (httpMethodFound.isPresent()) {
            final HttpMethod httpMethod = httpMethodFound.get();
            if (httpMethod.isRequestBodySupported()) {
                requestBody = getRequestBodyToSend(record, context, requestFlowFile, contentEncodingStrategy);
            } else {
                requestBody = null;
            }
        } else {
            requestBody = null;
        }
        requestBuilder.method(method, requestBody);

        setHeaderProperties(record, context, requestBuilder, requestFlowFile);
        return requestBuilder.build();
    }

    private Optional<HttpMethod> findRequestMethod(String method) {
        return Arrays.stream(HttpMethod.values())
                .filter(httpMethod -> httpMethod.name().equals(method))
                .findFirst();
    }

    private String getRequestMethod(final PropertyContext context, final FlowFile flowFile) {
        final String method = context.getProperty(HTTP_METHOD).evaluateAttributeExpressions(flowFile).getValue().toUpperCase();
        return trimToEmpty(method);
    }

    private RequestBody getRequestBodyToSend(final Record record, final ProcessContext context,
                                             final FlowFile requestFlowFile,
                                             final ContentEncodingStrategy contentEncodingStrategy
    ) throws Exception {
        final ComponentLog logger = getLogger();

        final String bodyContent;
        final String bodyType = context.getProperty(REQUEST_BODY_TYPE).getValue();
        if (bodyType.equals(BODY_TYPE_LITERAL.getValue())) {
            logger.debug("In literal body mode");
            bodyContent = context.getProperty(REQUEST_BODY_LITERAL).evaluateAttributeExpressions().getValue();
        } else if (bodyType.equals(BODY_TYPE_RECORD_PATH.getValue())) {
            logger.debug("In RecordPath body mode");
            final RecordPath recordPath = recordPathCache.getCompiled(context.getProperty(REQUEST_BODY_RECORD_PATH).evaluateAttributeExpressions().getValue());
            logger.debug("Found body record path: " + recordPath);
            final Optional<FieldValue> value = recordPath.evaluate(record).getSelectedFields().findFirst();

            if (value.isEmpty()) {
                throw new Exception("Record path \"" + recordPath + "\" did not resolve to a value for request body, skipping record");
            }

            bodyContent = value.get().getValue().toString();
        } else {
            throw new Exception("Unknown body type \"" + bodyType + "\", skipping record");
        }

        String evalContentType = context.getProperty(REQUEST_CONTENT_TYPE)
                .evaluateAttributeExpressions(requestFlowFile).getValue();
        final String contentType = StringUtils.isBlank(evalContentType) ? DEFAULT_CONTENT_TYPE : evalContentType;
        String formDataName = context.getProperty(REQUEST_FORM_DATA_NAME).evaluateAttributeExpressions(requestFlowFile).getValue();

        final boolean contentLengthUnknown = chunkedTransferEncoding || ContentEncodingStrategy.GZIP == contentEncodingStrategy;
        RequestBody requestBody = new RequestBody() {
            @Nullable
            @Override
            public MediaType contentType() {
                return MediaType.parse(contentType);
            }

            @Override
            public void writeTo(final BufferedSink sink) throws IOException {
                final BufferedSink outputSink = (ContentEncodingStrategy.GZIP == contentEncodingStrategy)
                        ? Okio.buffer(new GzipSink(sink))
                        : sink;

                outputSink.writeString(bodyContent, StandardCharsets.UTF_8);

                // Close Output Sink for gzip to write trailing bytes
                if (ContentEncodingStrategy.GZIP == contentEncodingStrategy) {
                    outputSink.close();
                }
            }

            @Override
            public long contentLength() {
                return contentLengthUnknown ? -1 : bodyContent.getBytes(StandardCharsets.UTF_8).length;
            }
        };

        if (StringUtils.isNotEmpty(formDataName)) {
            // we have form data
            MultipartBody.Builder builder = new Builder().setType(MultipartBody.FORM);
            builder.addFormDataPart(formDataName, "placeholder", requestBody);
            return builder.build();
        } else {
            return requestBody;
        }
    }

    private void setHeaderProperties(final Record record, final ProcessContext context, final Request.Builder requestBuilder, final FlowFile requestFlowFile) throws Exception {
        final String userAgent = trimToEmpty(context.getProperty(REQUEST_USER_AGENT).evaluateAttributeExpressions(requestFlowFile).getValue());
        requestBuilder.addHeader(HttpHeader.USER_AGENT.getHeader(), userAgent);

        if (context.getProperty(REQUEST_DATE_HEADER_ENABLED).asBoolean()) {
            final ZonedDateTime universalCoordinatedTimeNow = ZonedDateTime.now(ZoneOffset.UTC);
            requestBuilder.addHeader(HttpHeader.DATE.getHeader(), RFC_2616_DATE_TIME.format(universalCoordinatedTimeNow));
        }

        final ComponentLog logger = getLogger();

        final String headerType = context.getProperty(HTTP_HEADER_TYPE).getValue();
        for (String headerKey : dynamicPropertyNames) {
            String headerValue = context.getProperty(headerKey).evaluateAttributeExpressions(requestFlowFile).getValue();

            if (headerType.equals(HEADER_TYPE_LITERAL.getValue())) {
                logger.debug("In literal header mode, adding header {}: {}", headerKey, headerValue);
                requestBuilder.addHeader(headerKey, headerValue);
            } else if (headerType.equals(HEADER_TYPE_RECORD_PATH.getValue())) {
                logger.debug("In RecordPath header mode");
                final RecordPath recordPath = recordPathCache.getCompiled(headerValue);
                logger.debug("Found header record path: " + recordPath);
                final Optional<FieldValue> value = recordPath.evaluate(record).getSelectedFields().findFirst();

                if (value.isEmpty()) {
                    throw new Exception("Record path \"" + recordPath + "\" did not resolve to a value for header, skipping record");
                }

                final String recordPathValue = value.get().getValue().toString();
                logger.debug("Adding header {}: {}", headerKey, recordPathValue);
                requestBuilder.addHeader(headerKey, recordPathValue);
            } else {
                throw new Exception("Unknown header type \"" + headerType + "\", skipping record");
            }
        }
    }

    private static File getResponseCacheDirectory() throws IOException {
        return Files.createTempDirectory(InvokeHTTPRecord.class.getSimpleName()).toFile();
    }
}
