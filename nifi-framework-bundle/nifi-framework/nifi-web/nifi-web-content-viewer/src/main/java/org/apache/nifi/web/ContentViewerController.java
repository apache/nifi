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
package org.apache.nifi.web;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.ViewableContent.DisplayMode;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controller servlet for viewing content. This is responsible for generating
 * the markup for the header and footer of the page. Included in that is the
 * combo that allows the user to choose how they wait to view the data
 * (original, formatted, hex). If a data viewer is registered for the detected
 * content type, it will include the markup it generates in the response.
 */
public class ContentViewerController extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(ContentViewerController.class);

    // 1.5kb - multiple of 12 (3 bytes = 4 base 64 encoded chars)
    private final static int BUFFER_LENGTH = 1536;

    private static final String PROXY_CONTEXT_PATH_HTTP_HEADER = "X-ProxyContextPath";
    private static final String FORWARDED_CONTEXT_HTTP_HEADER = "X-Forwarded-Context";
    private static final String FORWARDED_PREFIX_HTTP_HEADER = "X-Forwarded-Prefix";

  /**
     * Gets the content and defers to registered viewers to generate the markup.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        // specify the charset in a response header
        response.addHeader("Content-Type", "text/html; charset=UTF-8");

        // get the content
        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute("nifi-content-access");

        final ContentRequestContext contentRequest;
        try {
            contentRequest = getContentRequest(request);
        } catch (final Exception e) {
            logger.warn("Content loading failed", e);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Content loading failed");
            return;
        }

        if (contentRequest.getDataUri() == null) {
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Data Reference URI must be specified");
            return;
        }

        // get the content
        final DownloadableContent downloadableContent;
        try {
            downloadableContent = contentAccess.getContent(contentRequest);
        } catch (final ResourceNotFoundException e) {
            logger.warn("Content not found", e);
            response.sendError(HttpURLConnection.HTTP_NOT_FOUND, "Content not found");
            return;
        } catch (final AccessDeniedException e) {
            logger.warn("Content access denied", e);
            response.sendError(HttpURLConnection.HTTP_FORBIDDEN, "Content access denied");
            return;
        } catch (final Exception e) {
            logger.warn("Content retrieval failed", e);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Content retrieval failed");
            return;
        }

        // determine how we want to view the data
        String mode = request.getParameter("mode");

        // if the name isn't set, use original
        if (mode == null) {
            mode = DisplayMode.Original.name();
        }

        // determine the display mode
        final DisplayMode displayMode;
        try {
            displayMode = DisplayMode.valueOf(mode);
        } catch (final IllegalArgumentException iae) {
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Display mode not supported [%s]".formatted(mode));
            return;
        }

        // buffer the content to support resetting in case we need to detect the content type or char encoding
        try (final BufferedInputStream bis = new BufferedInputStream(downloadableContent.getContent())) {
            final String mimeType;
            final String normalizedMimeType;

            // when standalone and we don't know the type is null as we were able to directly access the content bypassing the rest endpoint,
            // when clustered and we don't know the type set to octet stream since the content was retrieved from the node's rest endpoint
            if (downloadableContent.getType() == null || StringUtils.startsWithIgnoreCase(downloadableContent.getType(), MediaType.OCTET_STREAM.toString())) {
                // attempt to detect the content stream if we don't know what it is ()
                final DefaultDetector detector = new DefaultDetector();

                // create the stream for tika to process, buffered to support reseting
                final TikaInputStream tikaStream = TikaInputStream.get(bis);

                // provide a hint based on the filename
                final Metadata metadata = new Metadata();
                metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, downloadableContent.getFilename());

                // Get mime type
                final MediaType mediatype = detector.detect(tikaStream, metadata);
                mimeType = mediatype.toString();
            } else {
                mimeType = downloadableContent.getType();
            }

            // Extract only mime type and subtype from content type (anything after the first ; are parameters)
            // Lowercase so subsequent code does not need to implement case insensitivity
            normalizedMimeType = mimeType.split(";",2)[0].toLowerCase();

            // add attributes needed for the header
            request.setAttribute("filename", downloadableContent.getFilename());
            request.setAttribute("contentType", mimeType);

            // generate the header
            request.getRequestDispatcher("/WEB-INF/jsp/header.jsp").include(request, response);

            // remove the attributes needed for the header
            request.removeAttribute("filename");
            request.removeAttribute("contentType");

            // generate the markup for the content based on the display mode
            if (DisplayMode.Hex.equals(displayMode)) {
                final byte[] buffer = new byte[BUFFER_LENGTH];
                final int read = StreamUtils.fillBuffer(bis, buffer, false);

                // trim the byte array if necessary
                byte[] bytes = buffer;
                if (read != buffer.length) {
                    bytes = new byte[read];
                    System.arraycopy(buffer, 0, bytes, 0, read);
                }

                // convert bytes into the base 64 bytes
                final String base64 = Base64.encodeBase64String(bytes);

                // defer to the jsp
                request.setAttribute("content", base64);
                request.getRequestDispatcher("/WEB-INF/jsp/hexview.jsp").include(request, response);
            } else {
                // lookup a viewer for the content
                final String contentViewerUri = servletContext.getInitParameter(normalizedMimeType);

                // handle no viewer for content type
                if (contentViewerUri == null) {
                    request.getRequestDispatcher("/WEB-INF/jsp/no-viewer.jsp").include(request, response);
                } else {
                    // create a request attribute for accessing the content
                    request.setAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE, new ViewableContent() {
                        @Override
                        public InputStream getContentStream() {
                            return bis;
                        }

                        @Override
                        public String getContent() throws IOException {
                            // detect the charset
                            final CharsetDetector detector = new CharsetDetector();
                            detector.setText(bis);
                            detector.enableInputFilter(true);
                            final CharsetMatch match = detector.detect();

                            // ensure we were able to detect the charset
                            if (match == null) {
                                throw new IOException("Unable to detect character encoding.");
                            }

                            // convert the stream using the detected charset
                            return IOUtils.toString(bis, match.getName());
                        }

                        @Override
                        public ViewableContent.DisplayMode getDisplayMode() {
                            return displayMode;
                        }

                        @Override
                        public String getFileName() {
                            return downloadableContent.getFilename();
                        }

                        @Override
                        public String getContentType() {
                            return normalizedMimeType;
                        }

                        @Override
                        public String getRawContentType() {
                            return mimeType;
                        }
                    });

                    try {
                        final ServletContext viewerContext = (ServletContext) servletContext.getAttribute(contentViewerUri);
                        viewerContext.getRequestDispatcher("/view-content").include(request, response);
                    } catch (final Exception e) {
                        logger.error("Content preparation failed for Content Viewer [{}]", contentViewerUri, e);
                        response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Content preparation failed");
                        return;
                    }

                    // remove the request attribute
                    request.removeAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);
                }
            }

            // generate footer
            request.getRequestDispatcher("/WEB-INF/jsp/footer.jsp").include(request, response);
        }
    }

    /**
     * @param request request
     * @return Get the content request context based on the specified request
     */
    private ContentRequestContext getContentRequest(final HttpServletRequest request) {
        final String ref = request.getParameter("ref");
        final String clientId = request.getParameter("clientId");

        final UriBuilder refUriBuilder = UriBuilder.fromUri(ref);

        // base the data ref on the request parameter but ensure the scheme is based off the incoming request...
        // this is necessary for scenario's where the NiFi instance is behind a proxy running a different scheme
        refUriBuilder.scheme(request.getScheme());

        // If there is path context from a proxy, remove it since this request will be used inside the cluster
        final String proxyContextPath = getFirstHeaderValue(request, PROXY_CONTEXT_PATH_HTTP_HEADER, FORWARDED_CONTEXT_HTTP_HEADER, FORWARDED_PREFIX_HTTP_HEADER);
        if (StringUtils.isNotBlank(proxyContextPath)) {
            refUriBuilder.replacePath(StringUtils.substringAfter(UriBuilder.fromUri(ref).build().getPath(), proxyContextPath));
        }

        final URI refUri = refUriBuilder.build();

        final String query = refUri.getQuery();

        String rawClusterNodeId = null;
        if (query != null) {
            final String[] queryParameters = query.split("&");

            for (int i = 0; i < queryParameters.length; i++) {
                if (queryParameters[0].startsWith("clusterNodeId=")) {
                    rawClusterNodeId = StringUtils.substringAfterLast(queryParameters[0], "clusterNodeId=");
                }
            }
        }
        final String clusterNodeId = rawClusterNodeId;

        return new ContentRequestContext() {
            @Override
            public String getDataUri() {
                return refUri.toString();
            }

            @Override
            public String getClusterNodeId() {
                return clusterNodeId;
            }

            @Override
            public String getClientId() {
                return clientId;
            }
        };
    }

    /**
     * Returns the value for the first key discovered when inspecting the current request. Will
     * return null if there are no keys specified or if none of the specified keys are found.
     *
     * @param keys http header keys
     * @return the value for the first key found
     */
    private String getFirstHeaderValue(HttpServletRequest httpServletRequest, final String... keys) {
        if (keys == null) {
            return null;
        }

        for (final String key : keys) {
            final String value = httpServletRequest.getHeader(key);

            // if we found an entry for this key, return the value
            if (value != null) {
                return value;
            }
        }

        // unable to find any matching keys
        return null;
    }
}
