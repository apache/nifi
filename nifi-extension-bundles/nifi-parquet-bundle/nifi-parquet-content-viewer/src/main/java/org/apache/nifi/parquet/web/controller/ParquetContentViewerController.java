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
package org.apache.nifi.parquet.web.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.parquet.shared.NifiParquetInputFile;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.ContentRequestContext;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.HttpServletContentRequestContext;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParquetContentViewerController extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ParquetContentViewerController.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long MAX_CONTENT_SIZE = 1024 * 1024 * 2; // 10MB
    private static final int BUFFER_SIZE = 8 * 1024; // 8KB

    private static final byte NEWLINE = '\n';
    private static final byte COMMA = ',';
    private static final byte START_ARRAY = '[';
    private static final byte END_ARRAY = ']';

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        final ContentRequestContext requestContext = new HttpServletContentRequestContext(request);

        final ServletContext servletContext = request.getServletContext();
        final ContentAccess contentAccess = (ContentAccess) servletContext.getAttribute("nifi-content-access");
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

        try {
            // Convert InputStream to a seekable InputStream
            final byte[] data = getInputStreamBytes(downloadableContent.getContent());

            if (data.length == 0) {
                response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Content size is too large to display.");
                return;
            }

            final Configuration conf = new Configuration();

            // Allow older deprecated schemas
            conf.setBoolean("parquet.avro.readInt96AsFixed", true);

            final InputFile inputFile = new NifiParquetInputFile(new ByteArrayInputStream(data), data.length);
            try (
                    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build();
                    OutputStream outputStream = response.getOutputStream()) {
                writeRecords(outputStream, reader);
            }
        } catch (final Throwable t) {
            logger.warn("Unable to format FlowFile content", t);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to format FlowFile content");
        }
    }

    private void writeRecords(final OutputStream outputStream, final ParquetReader<GenericRecord> reader) throws IOException {
        // Format and write out each record
        GenericRecord record;
        boolean firstRecord = true;
        final ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();

        outputStream.write(START_ARRAY);
        outputStream.write(NEWLINE);
        while ((record = reader.read()) != null) {
            if (firstRecord) {
                firstRecord = false;
            } else {
                outputStream.write(COMMA);
                outputStream.write(NEWLINE);
            }

            final Object mappedRecord = recordToMap(record);
            final byte[] serializedRecord = objectWriter.writeValueAsBytes(mappedRecord);
            outputStream.write(serializedRecord);
        }
        outputStream.write(NEWLINE);
        outputStream.write(END_ARRAY);
    }

    private static Object recordToMap(final Object obj) {
        switch (obj) {
            case GenericRecord record -> {
                final Map<String, Object> map = new LinkedHashMap<>();
                for (final Schema.Field field : record.getSchema().getFields()) {
                    map.put(field.name(), recordToMap(record.get(field.name())));
                }
                return map;
            }
            case Collection<?> coll -> {
                final List<Object> list = new ArrayList<>();
                for (Object elem : coll) {
                    list.add(recordToMap(elem));
                }
                return list;
            }
            case GenericData.EnumSymbol enumSymbol -> {
                return enumSymbol.toString();
            }
            case org.apache.avro.util.Utf8 utf8 -> {
                return utf8.toString();
            }
            case null, default -> {
                return obj;
            }
        }
    }

    private byte[] getInputStreamBytes(final InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[BUFFER_SIZE];
            long totalRead = 0;

            while (totalRead < MAX_CONTENT_SIZE) {
                int bytesRead = inputStream.read(buffer, 0, BUFFER_SIZE);
                if (bytesRead == -1) {
                    break;
                }
                outputStream.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }

            // Return empty array if inputStream has more data beyond maxBytes
            if (totalRead >= MAX_CONTENT_SIZE && inputStream.read() != -1) {
                return new byte[0];
            }

            return outputStream.toByteArray();
        }
    }
}
