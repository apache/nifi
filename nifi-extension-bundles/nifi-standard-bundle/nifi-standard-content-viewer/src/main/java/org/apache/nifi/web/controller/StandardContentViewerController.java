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
import org.apache.nifi.stream.io.LimitingInputStream;
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
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class StandardContentViewerController extends HttpServlet {

    static final String CONTENT_ACCESS_ATTRIBUTE = "nifi-content-access";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int CONTENT_LENGTH_LIMIT = 10_485_760;

    private static final Map<String, ContentType> CONTENT_TYPES = Arrays.stream(ContentType.values())
            .collect(
                    Collectors.toMap(ContentType::name, Function.identity())
            );

    private static final Logger logger = LoggerFactory.getLogger(StandardContentViewerController.class);

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        final ContentRequestContext requestContext = new HttpServletContentRequestContext(request);

        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute(CONTENT_ACCESS_ATTRIBUTE);

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

        // Set Response Status before committing based on formatted or unformatted stream handling
        final int responseStatus = getResponseStatus(downloadableContent);
        response.setStatus(responseStatus);

        final LimitingInputStream contentStream = getContentStream(downloadableContent);

        final boolean formatted = Boolean.parseBoolean(request.getParameter("formatted"));
        if (formatted) {
            final ContentType formattedContentType = getFormattedContentType(request, downloadableContent.getType());
            if (formattedContentType == null) {
                response.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Unknown Content Type");
            } else {
                final String dataUri = requestContext.getDataUri();
                final long contentLength = downloadableContent.getContentLength();
                writeContentFormatted(dataUri, formattedContentType, contentStream, contentLength, response);
            }
        } else {
            contentStream.transferTo(response.getOutputStream());
        }
    }

    private LimitingInputStream getContentStream(final DownloadableContent downloadableContent) {
        final InputStream contentStream = downloadableContent.getContent();
        return new LimitingInputStream(contentStream, CONTENT_LENGTH_LIMIT);
    }

    private int getResponseStatus(final DownloadableContent downloadableContent) {
        final int responseStatus;

        final long contentLength = downloadableContent.getContentLength();
        if (contentLength > CONTENT_LENGTH_LIMIT) {
            responseStatus = HttpURLConnection.HTTP_PARTIAL;
        } else {
            responseStatus = HttpURLConnection.HTTP_OK;
        }

        return responseStatus;
    }

    private void writeContentFormatted(
            final String dataUri,
            final ContentType contentType,
            final LimitingInputStream contentStream,
            final long contentLength,
            final HttpServletResponse response
    ) throws IOException {
        try {
            switch (contentType) {
                case JSON: {
                    final Object objectJson = OBJECT_MAPPER.readValue(contentStream, Object.class);
                    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), objectJson);
                    break;
                }
                case XML: {
                    final StreamSource source = new StreamSource(contentStream);
                    try (OutputStream outputStream = new FormattingOutputStream(response.getOutputStream())) {
                        final StreamResult result = new StreamResult(outputStream);
                        final StandardTransformProvider transformProvider = new StandardTransformProvider();
                        transformProvider.setIndent(true);
                        transformProvider.setOmitXmlDeclaration(true);
                        transformProvider.transform(source, result);
                    }
                    break;
                }
                case AVRO: {
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
                    try (final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(contentStream, datumReader)) {
                        while (dataFileReader.hasNext()) {
                            final GenericData.Record record = dataFileReader.next();
                            final String formattedRecord = genericData.toString(record);
                            sb.append(formattedRecord);
                            sb.append(",");
                        }
                    }

                    if (sb.length() > 1) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    sb.append("]");
                    final String json = sb.toString();

                    final Object objectJson = OBJECT_MAPPER.readValue(json, Object.class);
                    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), objectJson);
                    break;
                }
                case YAML: {
                    Yaml yaml = new Yaml();
                    // Parse the YAML file
                    final Object yamlObject = yaml.load(contentStream);
                    DumperOptions options = new DumperOptions();
                    options.setIndent(2);
                    options.setPrettyFlow(true);

                    // Fix below - additional configuration
                    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
                    Yaml output = new Yaml(options);
                    output.dump(yamlObject, response.getWriter());
                    break;
                }
                case CSV:
                case TEXT: {
                    contentStream.transferTo(response.getOutputStream());
                    break;
                }
                default: {
                    response.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Unsupported Content Type: %s".formatted(contentType));
                }
            }
        } catch (final Throwable t) {
            final String message;

            if (contentLength > CONTENT_LENGTH_LIMIT) {
                message = "FlowFile Content-Length exceeds maximum allowed";
                logger.warn("Requested FlowFile [{}] Content-Length exceeds maximum allowed [{} bytes]", dataUri, CONTENT_LENGTH_LIMIT, t);
            } else {
                message = "FlowFile formatting failed";
                logger.warn("Requested FlowFile [{}] formatting failed for Content Type [{}]", dataUri, contentType, t);
            }

            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, message);
        }
    }

    private ContentType getFormattedContentType(final HttpServletRequest request, final String downloadableContentType) {
        final ContentType formattedContentType;

        final String mimeTypeDisplayName = request.getParameter("mimeTypeDisplayName");
        if (mimeTypeDisplayName == null) {
            formattedContentType = getFormattedContentType(downloadableContentType);
        } else {
            final String upperCasedContentType = mimeTypeDisplayName.toUpperCase();
            formattedContentType = CONTENT_TYPES.get(upperCasedContentType);
        }

        return formattedContentType;
    }

    private ContentType getFormattedContentType(final String contentType) {
        return switch (contentType) {
            case "application/json" -> ContentType.JSON;
            case "application/xml", "text/xml" -> ContentType.XML;
            case "application/avro-binary", "avro/binary", "application/avro+binary" -> ContentType.AVRO;
            case "text/x-yaml", "text/yaml", "text/yml", "application/x-yaml", "application/x-yml", "application/yaml",
                 "application/yml" -> ContentType.YAML;
            case "text/plain" -> ContentType.TEXT;
            case "text/csv" -> ContentType.CSV;
            case null, default -> null;
        };
    }

    private enum ContentType {
        AVRO,
        CSV,
        JSON,
        TEXT,
        XML,
        YAML
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
