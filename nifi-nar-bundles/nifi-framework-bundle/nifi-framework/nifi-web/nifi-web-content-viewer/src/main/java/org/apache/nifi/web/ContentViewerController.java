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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.ViewableContent.DisplayMode;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

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
            request.setAttribute("title", "Error");
            request.setAttribute("messages", "Unable to interpret content request.");

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
            return;
        }

        if (contentRequest.getDataUri() == null) {
            request.setAttribute("title", "Error");
            request.setAttribute("messages", "The data reference must be specified.");

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
            return;
        }

        // get the content
        final DownloadableContent downloadableContent;
        try {
            downloadableContent = contentAccess.getContent(contentRequest);
        } catch (final ResourceNotFoundException rnfe) {
            request.setAttribute("title", "Error");
            request.setAttribute("messages", "Unable to find the specified content");

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
            return;
        } catch (final AccessDeniedException ade) {
            request.setAttribute("title", "Access Denied");
            request.setAttribute("messages", "Unable to approve access to the specified content: " + ade.getMessage());

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
            return;
        } catch (final Exception e) {
            request.setAttribute("title", "Error");
            request.setAttribute("messages", "An unexpected error has occurred: " + e.getMessage());

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
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
            request.setAttribute("title", "Error");
            request.setAttribute("messages", "Invalid display mode: " + mode);

            // forward to the error page
            final ServletContext viewerContext = servletContext.getContext("/nifi");
            viewerContext.getRequestDispatcher("/message").forward(request, response);
            return;
        }

        // buffer the content to support resetting in case we need to detect the content type or char encoding
        try (final BufferedInputStream bis = new BufferedInputStream(downloadableContent.getContent());) {
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
                metadata.set(Metadata.RESOURCE_NAME_KEY, downloadableContent.getFilename());

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
                        // generate the content
                        final ServletContext viewerContext = servletContext.getContext(contentViewerUri);
                        viewerContext.getRequestDispatcher("/view-content").include(request, response);
                    } catch (final Exception e) {
                        String message = e.getMessage() != null ? e.getMessage() : e.toString();
                        message = "Unable to generate view of data: " + message;

                        // log the error
                        logger.error(message);
                        if (logger.isDebugEnabled()) {
                            logger.error(StringUtils.EMPTY, e);
                        }

                        // populate the request attributes
                        request.setAttribute("title", "Error");
                        request.setAttribute("messages", message);

                        // forward to the error page
                        final ServletContext viewerContext = servletContext.getContext("/nifi");
                        viewerContext.getRequestDispatcher("/message").forward(request, response);
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

        // base the data ref on the request parameter but ensure the scheme is based off the incoming request...
        // this is necessary for scenario's where the NiFi instance is behind a proxy running a different scheme
        final URI refUri = UriBuilder.fromUri(ref)
                .scheme(request.getScheme())
                .build();

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

            @Override
            public String getProxiedEntitiesChain() {
                return null;
            }
        };
    }
}
