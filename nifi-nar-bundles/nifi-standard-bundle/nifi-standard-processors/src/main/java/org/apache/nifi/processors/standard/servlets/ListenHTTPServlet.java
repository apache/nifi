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
package org.apache.nifi.processors.standard.servlets;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.standard.ListenHTTP;
import org.apache.nifi.processors.standard.ListenHTTP.FlowFileEntryTimeWrapper;
import org.apache.nifi.stream.io.StreamThrottler;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFileUnpackager;
import org.apache.nifi.util.FlowFileUnpackagerV1;
import org.apache.nifi.util.FlowFileUnpackagerV2;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.eclipse.jetty.server.Request;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Path("")
public class ListenHTTPServlet extends HttpServlet {

    private static final long serialVersionUID = 5329940480987723163L;

    public static final String FLOWFILE_CONFIRMATION_HEADER = "x-prefer-acknowledge-uri";
    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String DEFAULT_FOUND_SUBJECT = "none";
    public static final String APPLICATION_FLOW_FILE_V1 = "application/flowfile";
    public static final String APPLICATION_FLOW_FILE_V2 = "application/flowfile-v2";
    public static final String APPLICATION_FLOW_FILE_V3 = "application/flowfile-v3";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "flowfile-hold";
    public static final int FILES_BEFORE_CHECKING_DESTINATION_SPACE = 5;
    public static final String ACCEPT_HEADER_NAME = "Accept";
    public static final String ACCEPT_HEADER_VALUE = APPLICATION_FLOW_FILE_V3 + "," + APPLICATION_FLOW_FILE_V2 + "," + APPLICATION_FLOW_FILE_V1 + ",*/*;q=0.8";
    public static final String ACCEPT_ENCODING_NAME = "Accept-Encoding";
    public static final String ACCEPT_ENCODING_VALUE = "gzip";
    public static final String GZIPPED_HEADER = "flowfile-gzipped";
    public static final String PROTOCOL_VERSION_HEADER = "x-nifi-transfer-protocol-version";
    public static final String PROTOCOL_VERSION = "3";

    private final AtomicLong filesReceived = new AtomicLong(0L);
    private final AtomicBoolean spaceAvailable = new AtomicBoolean(true);

    private ComponentLog logger;
    private AtomicReference<ProcessSessionFactory> sessionFactoryHolder;
    private volatile ProcessContext processContext;
    private Pattern authorizedPattern;
    private Pattern headerPattern;
    private ConcurrentMap<String, FlowFileEntryTimeWrapper> flowFileMap;
    private StreamThrottler streamThrottler;
    private String basePath;
    private int returnCode;
    private long multipartRequestMaxSize;
    private int multipartReadBufferSize;

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ServletConfig config) throws ServletException {
        final ServletContext context = config.getServletContext();
        this.logger = (ComponentLog) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_LOGGER);
        this.sessionFactoryHolder = (AtomicReference<ProcessSessionFactory>) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER);
        this.processContext = (ProcessContext) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER);
        this.authorizedPattern = (Pattern) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN);
        this.headerPattern = (Pattern) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_HEADER_PATTERN);
        this.flowFileMap = (ConcurrentMap<String, FlowFileEntryTimeWrapper>) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_FLOWFILE_MAP);
        this.streamThrottler = (StreamThrottler) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_STREAM_THROTTLER);
        this.basePath = (String) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_BASE_PATH);
        this.returnCode = (int) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_RETURN_CODE);
        this.multipartRequestMaxSize = (long) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE);
        this.multipartReadBufferSize = (int) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE);
    }

    @Override
    protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        response.addHeader(ACCEPT_ENCODING_NAME, ACCEPT_ENCODING_VALUE);
        response.addHeader(ACCEPT_HEADER_NAME, ACCEPT_HEADER_VALUE);
        response.addHeader(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION);
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final ProcessContext context = processContext;

        ProcessSessionFactory sessionFactory;
        do {
            sessionFactory = sessionFactoryHolder.get();
            if (sessionFactory == null) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                }
            }
        } while (sessionFactory == null);

        final ProcessSession session = sessionFactory.createSession();
        String foundSubject = null;
        try {
            final long n = filesReceived.getAndIncrement() % FILES_BEFORE_CHECKING_DESTINATION_SPACE;
            if (n == 0 || !spaceAvailable.get()) {
                if (context.getAvailableRelationships().isEmpty()) {
                    spaceAvailable.set(false);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received request from " + request.getRemoteHost() + " but no space available; Indicating Service Unavailable");
                    }
                    response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    return;
                } else {
                    spaceAvailable.set(true);
                }
            }
            response.setHeader("Content-Type", MediaType.TEXT_PLAIN);

            final boolean contentGzipped = Boolean.parseBoolean(request.getHeader(GZIPPED_HEADER));

            final X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
            foundSubject = DEFAULT_FOUND_SUBJECT;
            if (certs != null && certs.length > 0) {
                for (final X509Certificate cert : certs) {
                    foundSubject = cert.getSubjectDN().getName();
                    if (authorizedPattern.matcher(foundSubject).matches()) {
                        break;
                    } else {
                        logger.warn("Rejecting transfer attempt from " + foundSubject + " because the DN is not authorized, host=" + request.getRemoteHost());
                        response.sendError(HttpServletResponse.SC_FORBIDDEN, "not allowed based on dn");
                        return;
                    }
                }
            }

            final String destinationVersion = request.getHeader(PROTOCOL_VERSION_HEADER);
            Integer protocolVersion = null;
            if (destinationVersion != null) {
                try {
                    protocolVersion = Integer.valueOf(destinationVersion);
                } catch (final NumberFormatException e) {
                    // Value was invalid. Treat as if the header were missing.
                }
            }

            final boolean destinationIsLegacyNiFi = (protocolVersion == null);
            final boolean createHold = Boolean.parseBoolean(request.getHeader(FLOWFILE_CONFIRMATION_HEADER));
            final String contentType = request.getContentType();

            final InputStream unthrottled = contentGzipped ? new GZIPInputStream(request.getInputStream()) : request.getInputStream();

            final InputStream in = (streamThrottler == null) ? unthrottled : streamThrottler.newThrottledInputStream(unthrottled);

            if (logger.isDebugEnabled()) {
                logger.debug("Received request from " + request.getRemoteHost() + ", createHold=" + createHold + ", content-type=" + contentType + ", gzip=" + contentGzipped);
            }

            Set<FlowFile> flowFileSet;
            if (!Strings.isNullOrEmpty(request.getContentType()) && request.getContentType().contains("multipart/form-data")) {
                flowFileSet = handleMultipartRequest(request, session, foundSubject);
            } else {
                flowFileSet = handleRequest(request, session, foundSubject, destinationIsLegacyNiFi, contentType, in);
            }
            proceedFlow(request, response, session, foundSubject, createHold, flowFileSet);
        } catch (final Throwable t) {
            handleException(request, response, session, foundSubject, t);
        }
    }

    private void handleException(final HttpServletRequest request, final HttpServletResponse response,
                                 final ProcessSession session, String foundSubject, final Throwable t) throws IOException {
        session.rollback();
        logger.error("Unable to receive file from Remote Host: [{}] SubjectDN [{}] due to {}", new Object[]{request.getRemoteHost(), foundSubject, t});
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, t.toString());
    }

    private Set<FlowFile> handleMultipartRequest(HttpServletRequest request, ProcessSession session, String foundSubject) throws IOException, IllegalStateException, ServletException {
        Set<FlowFile> flowFileSet = new HashSet<>();
        String tempDir = System.getProperty("java.io.tmpdir");
        request.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, new MultipartConfigElement(tempDir, multipartRequestMaxSize, multipartRequestMaxSize, multipartReadBufferSize));
        List<Part> requestParts = ImmutableList.copyOf(request.getParts());
        for (int i = 0; i < requestParts.size(); i++) {
            Part part = requestParts.get(i);
            FlowFile flowFile = session.create();
            try (OutputStream flowFileOutputStream = session.write(flowFile)) {
                StreamUtils.copy(part.getInputStream(), flowFileOutputStream);
            }
            flowFile = saveRequestDetailsAsAttributes(request, session, foundSubject, flowFile);
            flowFile = savePartDetailsAsAttributes(session, part, flowFile, i, requestParts.size());
            flowFileSet.add(flowFile);
        }
        return flowFileSet;
    }

    private FlowFile savePartDetailsAsAttributes(final ProcessSession session, final Part part, final FlowFile flowFile, final int sequenceNumber, final int allPartsCount) {
        final Map<String, String> attributes = new HashMap<>();
        for (String headerName : part.getHeaderNames()) {
            final String headerValue = part.getHeader(headerName);
            putAttribute(attributes, "http.headers.multipart." + headerName, headerValue);
        }
        putAttribute(attributes, "http.multipart.size", part.getSize());
        putAttribute(attributes, "http.multipart.content.type", part.getContentType());
        putAttribute(attributes, "http.multipart.name", part.getName());
        putAttribute(attributes, "http.multipart.filename", part.getSubmittedFileName());
        putAttribute(attributes, "http.multipart.fragments.sequence.number", sequenceNumber + 1);
        putAttribute(attributes, "http.multipart.fragments.total.number", allPartsCount);
        return session.putAllAttributes(flowFile, attributes);
    }

    private Set<FlowFile> handleRequest(final HttpServletRequest request, final ProcessSession session,
                                        String foundSubject, final boolean destinationIsLegacyNiFi, final String contentType, final InputStream in) {
        FlowFile flowFile = null;
        String holdUuid = null;
        final AtomicBoolean hasMoreData = new AtomicBoolean(false);
        final FlowFileUnpackager unpackager;
        if (APPLICATION_FLOW_FILE_V3.equals(contentType)) {
            unpackager = new FlowFileUnpackagerV3();
        } else if (APPLICATION_FLOW_FILE_V2.equals(contentType)) {
            unpackager = new FlowFileUnpackagerV2();
        } else if (APPLICATION_FLOW_FILE_V1.equals(contentType)) {
            unpackager = new FlowFileUnpackagerV1();
        } else {
            unpackager = null;
        }

        final Set<FlowFile> flowFileSet = new HashSet<>();

        do {
            final long startNanos = System.nanoTime();
            final Map<String, String> attributes = new HashMap<>();
            flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final BufferedOutputStream bos = new BufferedOutputStream(rawOut, 65536)) {
                        if (unpackager == null) {
                            IOUtils.copy(in, bos);
                            hasMoreData.set(false);
                        } else {
                            attributes.putAll(unpackager.unpackageFlowFile(in, bos));

                            if (destinationIsLegacyNiFi) {
                                if (attributes.containsKey("nf.file.name")) {
                                    // for backward compatibility with old nifi...
                                    attributes.put(CoreAttributes.FILENAME.key(), attributes.remove("nf.file.name"));
                                }

                                if (attributes.containsKey("nf.file.path")) {
                                    attributes.put(CoreAttributes.PATH.key(), attributes.remove("nf.file.path"));
                                }
                            }

                            hasMoreData.set(unpackager.hasMoreData());
                        }
                    }
                }
            });

            final long transferNanos = System.nanoTime() - startNanos;
            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);

            // put metadata on flowfile
            final String nameVal = request.getHeader(CoreAttributes.FILENAME.key());
            if (StringUtils.isNotBlank(nameVal)) {
                attributes.put(CoreAttributes.FILENAME.key(), nameVal);
            }

            String sourceSystemFlowFileIdentifier = attributes.get(CoreAttributes.UUID.key());
            if (sourceSystemFlowFileIdentifier != null) {
                sourceSystemFlowFileIdentifier = "urn:nifi:" + sourceSystemFlowFileIdentifier;

                // If we receveied a UUID, we want to give the FlowFile a new UUID and register the sending system's
                // identifier as the SourceSystemFlowFileIdentifier field in the Provenance RECEIVE event
                attributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = saveRequestDetailsAsAttributes(request, session, foundSubject, flowFile);
            session.getProvenanceReporter().receive(flowFile, request.getRequestURL().toString(), sourceSystemFlowFileIdentifier, "Remote DN=" + foundSubject, transferMillis);
            flowFileSet.add(flowFile);

            if (holdUuid == null) {
                holdUuid = flowFile.getAttribute(CoreAttributes.UUID.key());
            }
        } while (hasMoreData.get());
        return flowFileSet;
    }

    protected FlowFile saveRequestDetailsAsAttributes(final HttpServletRequest request, final ProcessSession session,
                                                      String foundSubject, FlowFile flowFile) {
        Map<String, String> attributes = new HashMap<>();
        addMatchingRequestHeaders(request, attributes);
        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.putAttribute(flowFile, "restlistener.remote.source.host", request.getRemoteHost());
        flowFile = session.putAttribute(flowFile, "restlistener.request.uri", request.getRequestURI());
        flowFile = session.putAttribute(flowFile, "restlistener.remote.user.dn", foundSubject);
        return flowFile;
    }

    private void addMatchingRequestHeaders(final HttpServletRequest request, final Map<String, String> attributes) {
        // put arbitrary headers on flow file
        for (Enumeration<String> headerEnum = request.getHeaderNames();
             headerEnum.hasMoreElements(); ) {
            String headerName = headerEnum.nextElement();
            if (headerPattern != null && headerPattern.matcher(headerName).matches()) {
                String headerValue = request.getHeader(headerName);
                attributes.put(headerName, headerValue);
            }
        }
    }

    protected void proceedFlow(final HttpServletRequest request, final HttpServletResponse response,
                               final ProcessSession session, String foundSubject, final boolean createHold,
                               final Set<FlowFile> flowFileSet) throws IOException, UnsupportedEncodingException {
        if (createHold) {
            String uuid = UUID.randomUUID().toString();

            if (flowFileMap.containsKey(uuid)) {
                uuid = UUID.randomUUID().toString();
            }

            final FlowFileEntryTimeWrapper wrapper = new FlowFileEntryTimeWrapper(session, flowFileSet, System.currentTimeMillis(), request.getRemoteHost());
            FlowFileEntryTimeWrapper previousWrapper;
            do {
                previousWrapper = flowFileMap.putIfAbsent(uuid, wrapper);
                if (previousWrapper != null) {
                    uuid = UUID.randomUUID().toString();
                }
            } while (previousWrapper != null);

            response.setStatus(HttpServletResponse.SC_SEE_OTHER);
            final String ackUri = "/" + basePath + "/holds/" + uuid;
            response.addHeader(LOCATION_HEADER_NAME, ackUri);
            response.addHeader(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE);
            response.getOutputStream().write(ackUri.getBytes("UTF-8"));
            if (logger.isDebugEnabled()) {
                logger.debug("Ingested {} from Remote Host: [{}] Port [{}] SubjectDN [{}]; placed hold on these {} files with ID {}",
                    new Object[]{flowFileSet, request.getRemoteHost(), request.getRemotePort(), foundSubject, flowFileSet.size(), uuid});
            }
        } else {
            response.setStatus(this.returnCode);
            logger.info("Received from Remote Host: [{}] Port [{}] SubjectDN [{}]; transferring to 'success'",
                new Object[]{request.getRemoteHost(), request.getRemotePort(), foundSubject});

            session.transfer(flowFileSet, ListenHTTP.RELATIONSHIP_SUCCESS);
            session.commit();
        }
    }

    private void putAttribute(final Map<String, String> map, final String key, final Object value) {
        if (value == null) {
            return;
        }

        putAttribute(map, key, value.toString());
    }

    private void putAttribute(final Map<String, String> map, final String key, final String value) {
        if (value == null) {
            return;
        }

        map.put(key, value);
    }
}
