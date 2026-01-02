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
package org.apache.nifi.web.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.ContentRequestContext;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.HttpServletContentRequestContext;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class StandardContentViewerController extends HttpServlet {

    static final String CONTENT_ACCESS_ATTRIBUTE = "nifi-content-access";

    private static final Logger logger = LoggerFactory.getLogger(StandardContentViewerController.class);

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        final ContentRequestContext requestContext = new HttpServletContentRequestContext(request);

        // get the content
        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute(CONTENT_ACCESS_ATTRIBUTE);

        // get the content
        final DownloadableContent downloadableContent;
        try {
            downloadableContent = contentAccess.getContent(requestContext);
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

        response.setStatus(HttpServletResponse.SC_OK);

        final boolean formatted = Boolean.parseBoolean(request.getParameter("formatted"));
        if (!formatted) {
            final InputStream contentStream = downloadableContent.getContent();
            contentStream.transferTo(response.getOutputStream());
            return;
        }

        // allow the user to drive the data type but fall back to the content type if necessary
        String displayName = request.getParameter("mimeTypeDisplayName");
        if (displayName == null) {
            final String contentType = downloadableContent.getType();
            displayName = getDisplayName(contentType);
        }

        if (displayName == null) {
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unknown content type");
            return;
        }

        try {
            switch (displayName) {
                case "json": {
                    // format json
                    final ObjectMapper mapper = new ObjectMapper();
                    final Object objectJson = mapper.readValue(downloadableContent.getContent(), Object.class);
                    mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), objectJson);
                    break;
                }
                case "xml": {
                    final StreamSource source = new StreamSource(downloadableContent.getContent());
                    try (OutputStream outputStream = new FormattingOutputStream(response.getOutputStream())) {
                        final StreamResult result = new StreamResult(outputStream);
                        final StandardTransformProvider transformProvider = new StandardTransformProvider();
                        transformProvider.setIndent(true);
                        transformProvider.setOmitXmlDeclaration(true);
                        transformProvider.transform(source, result);
                    }
                    break;
                }
                case "avro": {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    // Use Avro conversions to display logical type values in human readable way.
                    final GenericData genericData = new GenericData();
                    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
                    final DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(null, null, genericData);
                    try (final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(downloadableContent.getContent(), datumReader)) {
                        while (dataFileReader.hasNext()) {
                            final GenericData.Record record = dataFileReader.next();
                            final String formattedRecord = genericData.toString(record);
                            sb.append(formattedRecord);
                            sb.append(",");
                            // Do not format more than 10 MB of content.
                            if (sb.length() > 1024 * 1024 * 2) {
                                break;
                            }
                        }
                    }

                    if (sb.length() > 1) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    sb.append("]");
                    final String json = sb.toString();

                    final ObjectMapper mapper = new ObjectMapper();
                    final Object objectJson = mapper.readValue(json, Object.class);

                    mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), objectJson);
                    break;
                }
                case "yaml": {
                    Yaml yaml = new Yaml();
                    // Parse the YAML file
                    final Object yamlObject = yaml.load(downloadableContent.getContent());
                    DumperOptions options = new DumperOptions();
                    options.setIndent(2);
                    options.setPrettyFlow(true);

                    // Fix below - additional configuration
                    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
                    Yaml output = new Yaml(options);
                    output.dump(yamlObject, response.getWriter());
                    break;
                }
                case "csv":
                case "text": {
                    final InputStream contentStream = downloadableContent.getContent();
                    contentStream.transferTo(response.getOutputStream());
                    break;
                }
                default: {
                    response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unsupported content type: " + displayName);
                }
            }
        } catch (final Throwable t) {
            logger.warn("Unable to format FlowFile content", t);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to format FlowFile content");
        }
    }

    private String getDisplayName(final String contentType) {
        return switch (contentType) {
            case "application/json" -> "json";
            case "application/xml", "text/xml" -> "xml";
            case "application/avro-binary", "avro/binary", "application/avro+binary" -> "avro";
            case "text/x-yaml", "text/yaml", "text/yml", "application/x-yaml", "application/x-yml", "application/yaml",
                 "application/yml" -> "yaml";
            case "text/plain" -> "text";
            case "text/csv" -> "csv";
            case null, default -> null;
        };
    }

    private static class FormattingOutputStream extends FilterOutputStream {

        private static final byte LINE_FEED = 10;
        private static final byte SPACE = 32;

        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        private FormattingOutputStream(final OutputStream outputStream) {
            super(outputStream);
        }

        @Override
        public void write(final int currentByte) throws IOException {
            buffer.write(currentByte);
            if (currentByte == LINE_FEED) {
                processBuffer();
            }
        }

        private void processBuffer() throws IOException {
            final byte[] bytes = buffer.toByteArray();

            if (hasCharacters(bytes)) {
                super.out.write(bytes);
            }

            buffer.reset();
        }

        private boolean hasCharacters(final byte[] bytes) {
            boolean charactersFound = false;

            for (byte currentByte : bytes) {
                if (currentByte > SPACE) {
                    charactersFound = true;
                    break;
                }
            }

            return charactersFound;
        }
    }
}
