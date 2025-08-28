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

import jakarta.servlet.AsyncContext;
import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.standard.ListenHTTP;
import org.apache.nifi.processors.standard.ListenHTTP.FlowFileEntryTimeWrapper;
import org.apache.nifi.processors.standard.exception.ListenHttpException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.security.cert.PrincipalFormatter;
import org.apache.nifi.security.cert.StandardPrincipalFormatter;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFileUnpackager;
import org.apache.nifi.util.FlowFileUnpackagerV1;
import org.apache.nifi.util.FlowFileUnpackagerV2;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
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
    public static final String DEFAULT_FOUND_ISSUER = "none";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "flowfile-hold";
    public static final int FILES_BEFORE_CHECKING_DESTINATION_SPACE = 5;
    public static final String ACCEPT_HEADER_NAME = "Accept";
    public static final String ACCEPT_HEADER_VALUE = String.format("%s,%s,%s,%s,*/*;q=0.8",
            StandardFlowFileMediaType.VERSION_3.getMediaType(),
            StandardFlowFileMediaType.VERSION_2.getMediaType(),
            StandardFlowFileMediaType.VERSION_1.getMediaType(),
            StandardFlowFileMediaType.VERSION_UNSPECIFIED.getMediaType());
    public static final String ACCEPT_ENCODING_NAME = "Accept-Encoding";
    public static final String ACCEPT_ENCODING_VALUE = "gzip";
    public static final String GZIPPED_HEADER = "flowfile-gzipped";
    public static final String PROTOCOL_VERSION_HEADER = "x-nifi-transfer-protocol-version";
    public static final String PROTOCOL_VERSION = "3";
    protected static final String CONTENT_ENCODING_HEADER = "Content-Encoding";

    private final AtomicLong filesReceived = new AtomicLong(0L);
    private final AtomicBoolean spaceAvailable = new AtomicBoolean(true);

    private ComponentLog logger;
    private AtomicReference<ProcessSessionFactory> sessionFactoryHolder;
    private volatile ProcessContext processContext;
    private Pattern authorizedPattern;
    private Pattern authorizedIssuerPattern;
    private Pattern headerPattern;
    private ConcurrentMap<String, FlowFileEntryTimeWrapper> flowFileMap;
    private String basePath;
    private int returnCode;
    private long multipartRequestMaxSize;
    private int multipartReadBufferSize;
    private int port;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ServletConfig config) throws ServletException {
        final ServletContext context = config.getServletContext();
        this.logger = (ComponentLog) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_LOGGER);
        this.sessionFactoryHolder = (AtomicReference<ProcessSessionFactory>) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER);
        this.processContext = (ProcessContext) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER);
        this.authorizedPattern = (Pattern) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN);
        this.authorizedIssuerPattern = (Pattern) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_AUTHORITY_ISSUER_PATTERN);
        this.headerPattern = (Pattern) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_HEADER_PATTERN);
        this.flowFileMap = (ConcurrentMap<String, FlowFileEntryTimeWrapper>) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_FLOWFILE_MAP);
        this.basePath = (String) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_BASE_PATH);
        this.returnCode = (int) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_RETURN_CODE);
        this.multipartRequestMaxSize = (long) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE);
        this.multipartReadBufferSize = (int) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE);
        this.port = (int) context.getAttribute(ListenHTTP.CONTEXT_ATTRIBUTE_PORT);
        this.readerFactory = processContext.getProperty(ListenHTTP.RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = readerFactory != null
                ? processContext.getProperty(ListenHTTP.RECORD_WRITER).asControllerService(RecordSetWriterFactory.class) : null;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    @Override
    public void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        if (request.getLocalPort() == port) {
            response.addHeader(ACCEPT_ENCODING_NAME, ACCEPT_ENCODING_VALUE);
            response.addHeader(ACCEPT_HEADER_NAME, ACCEPT_HEADER_VALUE);
            response.addHeader(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION);
        } else {
            super.doHead(request, response);
        }
    }

    @Override
    public void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {

        if (request.getLocalPort() != port) {
            super.doPost(request, response);
            return;
        }

        final ProcessContext context = processContext;

        ProcessSessionFactory sessionFactory;
        do {
            sessionFactory = sessionFactoryHolder.get();
            if (sessionFactory == null) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException ignored) {
                }
            }
        } while (sessionFactory == null);

        final ProcessSession session = sessionFactory.createSession();
        String foundSubject = null;
        String foundIssuer = null;
        try {
            final long n = filesReceived.getAndIncrement() % FILES_BEFORE_CHECKING_DESTINATION_SPACE;
            if (n == 0 || !spaceAvailable.get()) {
                if (context.getAvailableRelationships().isEmpty()) {
                    spaceAvailable.set(false);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received request from {} but no space available; Indicating Service Unavailable", request.getRemoteHost());
                    }
                    response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    return;
                } else {
                    spaceAvailable.set(true);
                }
            }
            response.setHeader("Content-Type", MediaType.TEXT_PLAIN);

            final boolean flowFileGzipped = Boolean.parseBoolean(request.getHeader(GZIPPED_HEADER));
            final String contentEncoding = request.getHeader(CONTENT_ENCODING_HEADER);
            final boolean contentEncodingGzip = ACCEPT_ENCODING_VALUE.equals(contentEncoding);
            final boolean contentGzipped = flowFileGzipped || contentEncodingGzip;

            final X509Certificate[] certs = (X509Certificate[]) request.getAttribute("jakarta.servlet.request.X509Certificate");
            foundSubject = DEFAULT_FOUND_SUBJECT;
            foundIssuer = DEFAULT_FOUND_ISSUER;
            if (certs != null) {
                final PrincipalFormatter principalFormatter = StandardPrincipalFormatter.getInstance();
                for (final X509Certificate cert : certs) {
                    foundSubject = principalFormatter.getSubject(cert);
                    foundIssuer = principalFormatter.getIssuer(cert);
                    if (authorizedPattern.matcher(foundSubject).matches()) {
                        if (authorizedIssuerPattern.matcher(foundIssuer).matches()) {
                            break;
                        } else {
                            logger.warn("Access Forbidden [Issuer not authorized] Host [{}] Subject [{}] Issuer [{}]", request.getRemoteHost(), foundSubject, foundIssuer);
                            response.sendError(HttpServletResponse.SC_FORBIDDEN, "not allowed based on issuer dn");
                            return;
                        }
                    } else {
                        logger.warn("Access Forbidden [Subject not authorized] Host [{}] Subject [{}] Issuer [{}]", request.getRemoteHost(), foundSubject, foundIssuer);
                        response.sendError(HttpServletResponse.SC_FORBIDDEN, "not allowed based on subject dn");
                        return;
                    }
                }
            }

            final String destinationVersion = request.getHeader(PROTOCOL_VERSION_HEADER);
            Integer protocolVersion = null;
            if (destinationVersion != null) {
                try {
                    protocolVersion = Integer.valueOf(destinationVersion);
                } catch (final NumberFormatException ignored) {
                    // Value was invalid. Treat as if the header were missing.
                }
            }

            final boolean destinationIsLegacyNiFi = (protocolVersion == null);
            final boolean createHold = Boolean.parseBoolean(request.getHeader(FLOWFILE_CONFIRMATION_HEADER));
            final String contentType = request.getContentType();

            final InputStream in = contentGzipped ? new GZIPInputStream(request.getInputStream()) : request.getInputStream();

            if (logger.isDebugEnabled()) {
                logger.debug("Received request from {}, createHold={}, content-type={}, gzip={}", request.getRemoteHost(), createHold, contentType, contentGzipped);
            }

            Set<FlowFile> flowFileSet;
            if (StringUtils.isNotBlank(request.getContentType()) && request.getContentType().contains("multipart/form-data")) {
                try {
                    flowFileSet = handleMultipartRequest(request, session, foundSubject, foundIssuer);
                } finally {
                    deleteMultiPartFiles(request);
                }
            } else {
                flowFileSet = handleRequest(request, session, foundSubject, foundIssuer, destinationIsLegacyNiFi, contentType, in);
            }
            proceedFlow(request, response, session, foundSubject, foundIssuer, createHold, flowFileSet);
        } catch (final Throwable t) {
            handleException(request, response, session, foundSubject, foundIssuer, t);
        }
    }

    private void deleteMultiPartFiles(final HttpServletRequest request) {
        try {
            for (final Part part : request.getParts()) {
                part.delete();
            }
        } catch (final Exception e) {
            logger.warn("Delete MultiPart temporary files failed", e);
        }
    }

    private void handleException(final HttpServletRequest request, final HttpServletResponse response,
                                 final ProcessSession session, final String foundSubject, final String foundIssuer, final Throwable t) throws IOException {
        session.rollback();
        logger.error("Unable to receive file from Remote Host: [{}] SubjectDN [{}] IssuerDN [{}] due to {}", request.getRemoteHost(), foundSubject, foundIssuer, t);
        if (t instanceof ListenHttpException) {
            final int returnCode = ((ListenHttpException) t).getReturnCode();
            response.sendError(returnCode, t.toString());
        } else {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, t.toString());
        }
    }

    private Set<FlowFile> handleMultipartRequest(HttpServletRequest request, ProcessSession session, String foundSubject, String foundIssuer)
            throws IOException, IllegalStateException, ServletException {
        Set<FlowFile> flowFileSet = new HashSet<>();
        String tempDir = System.getProperty("java.io.tmpdir");
        request.setAttribute(ServletContextRequest.MULTIPART_CONFIG_ELEMENT, new MultipartConfigElement(tempDir, multipartRequestMaxSize, multipartRequestMaxSize, multipartReadBufferSize));
        int i = 0;
        final Collection<Part> requestParts = request.getParts();
        for (final Part part : requestParts) {
            FlowFile flowFile = session.create();
            try (
                    OutputStream flowFileOutputStream = session.write(flowFile);
                    InputStream partInputStream = part.getInputStream()
            ) {
                StreamUtils.copy(partInputStream, flowFileOutputStream);
            }
            flowFile = saveRequestDetailsAsAttributes(request, session, foundSubject, foundIssuer, flowFile);
            flowFile = savePartDetailsAsAttributes(session, part, flowFile, i, requestParts.size());
            flowFileSet.add(flowFile);
            i++;
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

    private Set<FlowFile> handleRequest(final HttpServletRequest request, final ProcessSession session, String foundSubject, String foundIssuer,
                                        final boolean destinationIsLegacyNiFi, final String contentType, final InputStream in) throws IOException {
        FlowFile flowFile;
        final AtomicBoolean hasMoreData = new AtomicBoolean(false);
        final FlowFileUnpackager unpackager = getFlowFileUnpackager(contentType);

        final Set<FlowFile> flowFileSet = new HashSet<>();

        do {
            final long startNanos = System.nanoTime();
            final Map<String, String> attributes = new HashMap<>();
            flowFile = session.create();

            final OutputStream out = session.write(flowFile);

            try (final BufferedOutputStream bos = new BufferedOutputStream(out, 65536)) {
                if (unpackager == null) {
                    if (isRecordProcessing()) {
                        processRecord(in, flowFile, out);
                    } else {
                        IOUtils.copy(in, bos);
                        hasMoreData.set(false);
                    }
                } else {
                    attributes.putAll(unpackager.unpackageFlowFile(in, bos));

                    hasMoreData.set(unpackager.hasMoreData());
                }
            }


            final long transferNanos = System.nanoTime() - startNanos;
            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);

            // put metadata on flowfile
            final String nameVal = request.getHeader(CoreAttributes.FILENAME.key());
            // Favor filename extracted from unpackager over filename in header
            if (StringUtils.isBlank(attributes.get(CoreAttributes.FILENAME.key())) && StringUtils.isNotBlank(nameVal)) {
                attributes.put(CoreAttributes.FILENAME.key(), nameVal);
            }

            String sourceSystemFlowFileIdentifier = attributes.remove(CoreAttributes.UUID.key());
            if (sourceSystemFlowFileIdentifier != null) {
                sourceSystemFlowFileIdentifier = "urn:nifi:" + sourceSystemFlowFileIdentifier; //NOPMD
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = saveRequestDetailsAsAttributes(request, session, foundSubject, foundIssuer, flowFile);
            final String details = String.format("Remote DN=%s, Issuer DN=%s", foundSubject, foundIssuer);
            session.getProvenanceReporter().receive(flowFile, request.getRequestURL().toString(), sourceSystemFlowFileIdentifier, details, transferMillis);
            flowFileSet.add(flowFile);

        } while (hasMoreData.get());
        return flowFileSet;
    }

    protected void proceedFlow(final HttpServletRequest request, final HttpServletResponse response,
                               final ProcessSession session, final String foundSubject, final String foundIssuer, final boolean createHold,
                               final Set<FlowFile> flowFileSet) throws IOException {
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
            response.getOutputStream().write(ackUri.getBytes(StandardCharsets.UTF_8));
            if (logger.isDebugEnabled()) {
                logger.debug("Ingested {} from Remote Host: [{}] Port [{}] SubjectDN [{}] IssuerDN [{}]; placed hold on these {} files with ID {}",
                        flowFileSet, request.getRemoteHost(), request.getRemotePort(), foundSubject, foundIssuer, flowFileSet.size(), uuid);
            }
        } else {
            logger.info("Received from Remote Host: [{}] Port [{}] SubjectDN [{}] IssuerDN [{}]; transferring to 'success'",
                    request.getRemoteHost(), request.getRemotePort(), foundSubject, foundIssuer);

            session.transfer(flowFileSet, ListenHTTP.RELATIONSHIP_SUCCESS);

            final AsyncContext asyncContext = request.startAsync();
            session.commitAsync(() -> {
                        response.setStatus(this.returnCode);
                        asyncContext.complete();
                    }, t -> {
                        logger.error("Failed to commit session. Returning error response to Remote Host: [{}] Port [{}] SubjectDN [{}] IssuerDN [{}]",
                                request.getRemoteHost(), request.getRemotePort(), foundSubject, foundIssuer, t);
                        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        asyncContext.complete();
                    }
            );
        }
    }

    protected FlowFile saveRequestDetailsAsAttributes(final HttpServletRequest request, final ProcessSession session,
                                                      final String foundSubject, final String foundIssuer, FlowFile flowFile) {
        Map<String, String> attributes = new HashMap<>();
        addMatchingRequestHeaders(request, attributes);
        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.putAttribute(flowFile, "restlistener.remote.source.host", request.getRemoteHost());
        flowFile = session.putAttribute(flowFile, "restlistener.request.uri", request.getRequestURI());
        flowFile = session.putAttribute(flowFile, "restlistener.remote.user.dn", foundSubject);
        flowFile = session.putAttribute(flowFile, "restlistener.remote.issuer.dn", foundIssuer);
        return flowFile;
    }

    private void processRecord(InputStream in, FlowFile flowFile, OutputStream out) {
        try (final RecordReader reader = readerFactory.createRecordReader(flowFile, new BufferedInputStream(in), logger)) {
            final RecordSet recordSet = reader.createRecordSet();
            try (final RecordSetWriter writer = writerFactory.createWriter(logger, reader.getSchema(), out, flowFile)) {
                writer.write(recordSet);
            }
        } catch (IOException | MalformedRecordException e) {
            throw new ListenHttpException("Could not process record.", e, HttpServletResponse.SC_BAD_REQUEST);
        } catch (SchemaNotFoundException e) {
            throw new ListenHttpException("Could not find schema.", e, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    private FlowFileUnpackager getFlowFileUnpackager(String contentType) {
        final FlowFileUnpackager unpackager;
        if (StandardFlowFileMediaType.VERSION_3.getMediaType().equals(contentType)) {
            unpackager = new FlowFileUnpackagerV3();
        } else if (StandardFlowFileMediaType.VERSION_2.getMediaType().equals(contentType)) {
            unpackager = new FlowFileUnpackagerV2();
        } else if (Strings.CS.startsWith(contentType, StandardFlowFileMediaType.VERSION_UNSPECIFIED.getMediaType())) {
            unpackager = new FlowFileUnpackagerV1();
        } else {
            unpackager = null;
        }
        return unpackager;
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

    private boolean isRecordProcessing() {
        return readerFactory != null && writerFactory != null;
    }
}
