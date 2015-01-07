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

import static org.apache.commons.lang3.StringUtils.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@SupportsBatching
@Tags({"http", "https", "rest", "client"})
@CapabilityDescription("An HTTP client processor which converts FlowFile attributes to HTTP headers, with configurable HTTP method, url, etc.")
public final class InvokeHTTP extends AbstractProcessor {

    //-- properties --//
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        Set<String> contextIdentifiers = getControllerServiceLookup().getControllerServiceIdentifiers(SSLContextService.class);

        PropertyDescriptor contextServiceSelector = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(Config.PROP_SSL_CONTEXT_SERVICE)
                .allowableValues(contextIdentifiers)
                .build();

        List<PropertyDescriptor> list = new ArrayList<>(Config.PROPERTIES);
        list.add(2, contextServiceSelector);

        return Collections.unmodifiableList(list);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (Config.PROP_TRUSTED_HOSTNAME.getName().equalsIgnoreCase(propertyDescriptorName)) {
            return Config.PROP_TRUSTED_HOSTNAME;
        }
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    //-- relationships --//
    @Override
    public Set<Relationship> getRelationships() {
        return Config.RELATIONSHIPS;
    }

    //-- class properties --//
    final AtomicReference<SSLContext> sslContextRef = new AtomicReference<>();
    final AtomicReference<Pattern> attributesToSendRef = new AtomicReference<>();

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        newValue = StringUtils.trimToEmpty(newValue);

        // listen for the SSL Context Service property and retrieve the SSLContext from the controller service.
        if (Config.PROP_SSL_CONTEXT_SERVICE.getName().equalsIgnoreCase(descriptor.getName())) {
            if (newValue.isEmpty()) {
                sslContextRef.set(null);
            } else {
                SSLContextService svc = (SSLContextService) getControllerServiceLookup().getControllerService(newValue);
                sslContextRef.set(svc.createSSLContext(ClientAuth.NONE));  // ClientAuth is only useful for servers, not clients.
                getLogger().info("Loading SSL configuration from keystore={} and truststore={}", new Object[]{svc.getKeyStoreFile(), svc.getTrustStoreFile()});
            }
        }

        // compile the attributes-to-send filter pattern
        if (Config.PROP_ATTRIBUTES_TO_SEND.getName().equalsIgnoreCase(descriptor.getName())) {
            if (newValue.isEmpty()) {
                attributesToSendRef.set(null);
            } else {
                attributesToSendRef.set(Pattern.compile(newValue));
            }
        }

    }

    //-- processing --//
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowfiles = session.get(Config.MAX_RESULTS_PER_THREAD);
        if (flowfiles.isEmpty()) {
            context.yield();
            return;
        }

        for (FlowFile flowfile : flowfiles) {
            Transaction transaction = new Transaction(
                    getLogger(), sslContextRef, attributesToSendRef, context, session, flowfile
            );
            transaction.process();
        }
    }

    /**
     *
     * Stores properties, relationships, configuration values, hard coded
     * strings, magic numbers, etc.
     *
     *
     */
    public interface Config {

        //-- magic numbers --//
        int MAX_RESULTS_PER_THREAD = 50;

        //-- flowfile attribute keys returned after reading the response --//
        String STATUS_CODE = "invokehttp.status.code";
        String STATUS_MESSAGE = "invokehttp.status.message";
        String RESPONSE_BODY = "invokehttp.response.body";
        String REQUEST_URL = "invokehttp.request.url";
        String TRANSACTION_ID = "invokehttp.tx.id";
        String REMOTE_DN = "invokehttp.remote.dn";

        // Set of flowfile attributes which we generally always ignore during
        // processing, including when converting http headers, copying attributes, etc.
        // This set includes our strings defined above as well as some standard flowfile
        // attributes.
        Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                STATUS_CODE, STATUS_MESSAGE, RESPONSE_BODY, REQUEST_URL, TRANSACTION_ID, REMOTE_DN,
                "uuid", "filename", "path"
        )));

        //-- properties --//
        PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
                .name("HTTP Method")
                .description("HTTP request method (GET, POST, PUT, DELETE, HEAD, OPTIONS).")
                .required(true)
                .defaultValue("GET")
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
                .name("Remote URL")
                .description("Remote URL which will be connected to, including scheme, host, port, path.")
                .required(true)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
                .name("Connection Timeout")
                .description("Max wait time for connection to remote service.")
                .required(true)
                .defaultValue("5 secs")
                .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                .build();

        PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
                .name("Read Timeout")
                .description("Max wait time for response from remote service.")
                .required(true)
                .defaultValue("15 secs")
                .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                .build();

        PropertyDescriptor PROP_DATE_HEADER = new PropertyDescriptor.Builder()
                .name("Include Date Header")
                .description("Include an RFC-2616 Date header in the request.")
                .required(true)
                .defaultValue("True")
                .allowableValues("True", "False")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .build();

        PropertyDescriptor PROP_FOLLOW_REDIRECTS = new PropertyDescriptor.Builder()
                .name("Follow Redirects")
                .description("Follow HTTP redirects issued by remote server.")
                .required(true)
                .defaultValue("True")
                .allowableValues("True", "False")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .build();

        PropertyDescriptor PROP_ATTRIBUTES_TO_SEND = new PropertyDescriptor.Builder()
                .name("Attributes to Send")
                .description("Regular expression that defines which attributes to send as HTTP headers in the request. "
                        + "If not defined, no attributes are sent as headers.")
                .required(false)
                .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
                .build();

        List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
                PROP_METHOD,
                PROP_URL,
                PROP_CONNECT_TIMEOUT,
                PROP_READ_TIMEOUT,
                PROP_DATE_HEADER,
                PROP_FOLLOW_REDIRECTS,
                PROP_ATTRIBUTES_TO_SEND
        ));

        // The allowableValues of the SSL Context Service property is dynamically populated at run time.
        PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
                .name("SSL Context Service")
                .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
                .required(false)
                .addValidator(StandardValidators.createControllerServiceExistsValidator(SSLContextService.class))
                .build();

        // property to allow the hostname verifier to be overridden
        // this is a "hidden" property - it's configured using a dynamic user property
        PropertyDescriptor PROP_TRUSTED_HOSTNAME = new PropertyDescriptor.Builder()
                .name("Trusted Hostname")
                .description("Bypass the normal truststore hostname verifier to allow the specified (single) remote hostname as trusted "
                        + "Enabling this property has MITM security implications, use wisely. Only valid with SSL (HTTPS) connections.")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();

        //-- relationships --//
        Relationship REL_SUCCESS_REQ = new Relationship.Builder()
                .name("Original")
                .description("Original FlowFile will be routed upon success (2xx status codes).")
                .build();

        Relationship REL_SUCCESS_RESP = new Relationship.Builder()
                .name("Response")
                .description("Response FlowFile will be routed upon success (2xx status codes).")
                .build();

        Relationship REL_RETRY = new Relationship.Builder()
                .name("Retry")
                .description("FlowFile will be routed on any status code that can be retried (5xx status codes).")
                .build();

        Relationship REL_NO_RETRY = new Relationship.Builder()
                .name("No Retry")
                .description("FlowFile will be routed on any status code that should NOT be retried (1xx, 3xx, 4xx status codes).")
                .build();

        Relationship REL_FAILURE = new Relationship.Builder()
                .name("Failure")
                .description("FlowFile will be routed on any type of connection failure, timeout or general exception.")
                .build();

        Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                REL_SUCCESS_REQ, REL_SUCCESS_RESP, REL_RETRY, REL_NO_RETRY, REL_FAILURE
        )));

    }

    /**
     * A single invocation of an HTTP request/response from the InvokeHTTP
     * processor. This class encapsulates the entirety of the flowfile
     * processing.
     * <p>
     * This class is not thread safe and is created new for every flowfile
     * processed.
     */
    private static class Transaction implements Config {

        /**
         * Pattern used to compute RFC 2616 Dates (#sec3.3.1). This format is
         * used by the HTTP Date header and is optionally sent by the processor.
         * This date is effectively an RFC 822/1123 date string, but HTTP
         * requires it to be in GMT (preferring the literal 'GMT' string).
         */
        private static final String rfc1123 = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";
        private static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern(rfc1123).withLocale(Locale.US).withZoneUTC();

        /**
         * Every request/response cycle from this client has a unique
         * transaction id which will be stored as a flowfile attribute. This
         * generator is used to create the id.
         */
        private static final AtomicLong txIdGenerator = new AtomicLong();

        private static final Charset utf8 = Charset.forName("UTF-8");

        private final ProcessorLog logger;
        private final AtomicReference<SSLContext> sslContextRef;
        private final AtomicReference<Pattern> attributesToSendRef;
        private final ProcessContext context;
        private final ProcessSession session;

        private final long txId = txIdGenerator.incrementAndGet();

        private FlowFile request;
        private FlowFile response;
        private HttpURLConnection conn;

        private int statusCode;
        private String statusMessage;

        public Transaction(
                ProcessorLog logger,
                AtomicReference<SSLContext> sslContextRef,
                AtomicReference<Pattern> attributesToSendRef,
                ProcessContext context,
                ProcessSession session,
                FlowFile request) {

            this.logger = logger;
            this.sslContextRef = sslContextRef;
            this.attributesToSendRef = attributesToSendRef;
            this.context = context;
            this.session = session;
            this.request = request;
        }

        public void process() {

            try {
                openConnection();
                sendRequest();
                readResponse();
                transfer();

            } catch (Throwable t) {
                // log exception
                logger.error("Routing to {} due to exception: {}", new Object[]{REL_FAILURE.getName(), t}, t);

                // penalize
                request = session.penalize(request);

                // transfer original to failure
                session.transfer(request, REL_FAILURE);

                // cleanup response flowfile, if applicable
                try {
                    if (response != null) {
                        session.remove(response);
                    }
                } catch (Throwable t1) {
                    logger.error("Could not cleanup response flowfile due to exception: {}", new Object[]{t1}, t1);
                }

            }
        }

        private void openConnection() throws IOException {
            // read the url property from the context
            String urlstr = trimToEmpty(context.getProperty(PROP_URL).evaluateAttributeExpressions(request).getValue());
            URL url = new URL(urlstr);

            // create the connection
            conn = (HttpURLConnection) url.openConnection();

            // set the request method
            String method = trimToEmpty(context.getProperty(PROP_METHOD).evaluateAttributeExpressions(request).getValue()).toUpperCase();
            conn.setRequestMethod(method);

            // set timeouts
            conn.setConnectTimeout(context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            conn.setReadTimeout(context.getProperty(PROP_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());

            // set whether to follow redirects
            conn.setInstanceFollowRedirects(context.getProperty(PROP_FOLLOW_REDIRECTS).asBoolean());

            // special handling for https
            if (conn instanceof HttpsURLConnection) {
                HttpsURLConnection sconn = (HttpsURLConnection) conn;

                // check if the ssl context is set
                SSLContext sslContext = sslContextRef.get();
                if (sslContext != null) {
                    sconn.setSSLSocketFactory(sslContext.getSocketFactory());
                }

                // check the trusted hostname property and override the HostnameVerifier
                String trustedHostname = trimToEmpty(context.getProperty(PROP_TRUSTED_HOSTNAME).getValue());
                if (!trustedHostname.isEmpty()) {
                    sconn.setHostnameVerifier(new OverrideHostnameVerifier(trustedHostname, sconn.getHostnameVerifier()));
                }
            }

        }

        private void sendRequest() throws IOException {
            // set the http request properties using flowfile attribute values
            setRequestProperties();

            // log request
            logRequest();

            // we only stream data for POST and PUT requests
            String method = conn.getRequestMethod().toUpperCase();
            if ("POST".equals(method) || "PUT".equals(method)) {
                conn.setDoOutput(true);
                conn.setFixedLengthStreamingMode(request.getSize());

                // write the flowfile contents to the output stream
                try (OutputStream os = new BufferedOutputStream(conn.getOutputStream())) {
                    session.exportTo(request, os);
                }

                // emit provenance event
                session.getProvenanceReporter().send(request, conn.getURL().toExternalForm());
            }

        }

        private void readResponse() throws IOException {

            // output the raw response headers (DEBUG level only)
            logResponse();

            // store the status code and message
            statusCode = conn.getResponseCode();
            statusMessage = conn.getResponseMessage();

            // always write the status attributes to the request flowfile
            request = writeStatusAttributes(request);

            // read from the appropriate input stream
            try (InputStream is = getResponseStream()) {

                // if not successful, store the response body into a flowfile attribute
                if (!isSuccess()) {
                    String body = trimToEmpty(toString(is, utf8));
                    request = session.putAttribute(request, RESPONSE_BODY, body);
                }

                // if successful, store the response body as the flowfile payload
                // we include additional flowfile attributes including the reponse headers
                // and the status codes.
                if (isSuccess()) {
                    // clone the flowfile to capture the response
                    response = session.clone(request);

                    // write the status attributes
                    response = writeStatusAttributes(response);

                    // write the response headers as attributes
                    // this will overwrite any existing flowfile attributes
                    response = session.putAllAttributes(response, convertAttributesFromHeaders());

                    // transfer the message body to the payload
                    // can potentially be null in edge cases
                    if (is != null) {
                        response = session.importFrom(is, response);
                    }

                    // invoke provenance events
                    session.getProvenanceReporter().receive(response, conn.getURL().toExternalForm());

                }

            }

        }

        private void transfer() throws IOException {
            // check if we should penalize the request
            if (!isSuccess()) {
                request = session.penalize(request);
            }

            // log the status codes from the response
            logger.info("Request to {} returned status code {} for {}", new Object[]{
                conn.getURL().toExternalForm(),
                statusCode,
                request
            });

            // transfer to the correct relationship
            // 2xx -> SUCCESS
            if (isSuccess()) {
                // we have two flowfiles to transfer
                session.transfer(request, REL_SUCCESS_REQ);
                session.transfer(response, REL_SUCCESS_RESP);

                // 5xx -> RETRY
            } else if (statusCode / 100 == 5) {
                session.transfer(request, REL_RETRY);

                // 1xx, 3xx, 4xx -> NO RETRY
            } else {
                session.transfer(request, REL_NO_RETRY);
            }

        }

        private void setRequestProperties() {

            // check if we should send the a Date header with the request
            if (context.getProperty(PROP_DATE_HEADER).asBoolean()) {
                conn.setRequestProperty("Date", getDateValue());
            }

            // iterate through the flowfile attributes, adding any attribute that
            // matches the attributes-to-send pattern. if the pattern is not set
            // (it's an optional property), ignore that attribute entirely
            Pattern p = attributesToSendRef.get();
            if (p != null) {
                Map<String, String> attributes = request.getAttributes();
                Matcher m = p.matcher("");
                for (Map.Entry<String, String> entry : attributes.entrySet()) {
                    String key = trimToEmpty(entry.getKey());
                    String val = trimToEmpty(entry.getValue());

                    // don't include any of the ignored attributes
                    if (IGNORED_ATTRIBUTES.contains(key)) {
                        continue;
                    }

                    // check if our attribute key matches the pattern
                    // if so, include in the request as a header
                    m.reset(key);
                    if (m.matches()) {
                        conn.setRequestProperty(key, val);
                    }
                }
            }
        }

        /**
         * Returns a Map of flowfile attributes from the response http headers.
         * Multivalue headers are naively converted to comma separated strings.
         */
        private Map<String, String> convertAttributesFromHeaders() throws IOException {
            // create a new hashmap to store the values from the connection
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : conn.getHeaderFields().entrySet()) {
                String key = entry.getKey();
                if (key == null) {
                    continue;
                }

                List<String> values = entry.getValue();

                // we ignore any headers with no actual values (rare)
                if (values == null || values.isEmpty()) {
                    continue;
                }

                // create a comma separated string from the values, this is stored in the map
                String value = csv(values);

                // put the csv into the map
                map.put(key, value);
            }

            if (conn instanceof HttpsURLConnection) {
                HttpsURLConnection sconn = (HttpsURLConnection) conn;
                // this should seemingly not be required, but somehow the state of the jdk client is messed up
                // when retrieving SSL certificate related information if connect() has not been called previously.
                sconn.connect();
                map.put(REMOTE_DN, sconn.getPeerPrincipal().getName());
            }

            return map;
        }

        private boolean isSuccess() throws IOException {
            if (statusCode == 0) {
                throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
            }
            return statusCode / 100 == 2;
        }

        private void logRequest() {
            logger.debug("\nRequest to remote service:\n\t{}\n{}", new Object[]{
                conn.getURL().toExternalForm(),
                getLogString(conn.getRequestProperties())
            });
        }

        private void logResponse() {
            logger.debug("\nResponse from remote service:\n\t{}\n{}", new Object[]{
                conn.getURL().toExternalForm(),
                getLogString(conn.getHeaderFields())
            });
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
         * Convert a collection of string values into a overly simple comma
         * separated string.
         *
         * Does not handle the case where the value contains the delimiter. i.e.
         * if a value contains a comma, this method does nothing to try and
         * escape or quote the value, in traditional csv style.
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
         * Return the current datetime as an RFC 1123 formatted string in the
         * GMT tz.
         */
        private String getDateValue() {
            return dateFormat.print(System.currentTimeMillis());
        }

        /**
         * Returns a string from the input stream using the specified character
         * encoding.
         */
        private String toString(InputStream is, Charset charset) throws IOException {
            if (is == null) {
                return "";
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int len;
            while ((len = is.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
            return new String(out.toByteArray(), charset);
        }

        /**
         * Returns the input stream to use for reading from the remote server.
         * We're either going to want the inputstream or errorstream,
         * effectively depending on the status code.
         * <p>
         * This method can return null if there is no inputstream to read from.
         * For example, if the remote server did not send a message body. eg.
         * 204 No Content or 304 Not Modified
         */
        private InputStream getResponseStream() {
            try {
                InputStream is = conn.getErrorStream();
                if (is == null) {
                    is = conn.getInputStream();
                }
                return new BufferedInputStream(is);

            } catch (IOException e) {
                logger.warn("Response stream threw an exception: {}", new Object[]{e}, e);
                return null;
            }
        }

        /**
         * Writes the status attributes onto the flowfile, returning the
         * flowfile that was updated.
         */
        private FlowFile writeStatusAttributes(FlowFile flowfile) {
            flowfile = session.putAttribute(flowfile, STATUS_CODE, String.valueOf(statusCode));
            flowfile = session.putAttribute(flowfile, STATUS_MESSAGE, statusMessage);
            flowfile = session.putAttribute(flowfile, REQUEST_URL, conn.getURL().toExternalForm());
            flowfile = session.putAttribute(flowfile, TRANSACTION_ID, Long.toString(txId));
            return flowfile;
        }

        /**
         *
         */
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

}
