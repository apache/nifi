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
package org.apache.parquet.web.controller;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.ContentRequestContext;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.HttpServletContentRequestContext;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.nifi.parquet.shared.NifiParquetInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

public class ParquetContentViewerController extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ParquetContentViewerController.class);
    private static final long MAX_CONTENT_SIZE = 1024 * 1024 * 2; // 10MB
    private static final int BUFFER_SIZE = 8 * 1024; // 8KB
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    static {
        objectMapper.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

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
            //Convert InputStream to a seekable InputStream
            byte[] data = getInputStreamBytes(downloadableContent.getContent());

            if (data.length == 0) {
                response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Content size is too large to display.");
                return;
            }

            Configuration conf = new Configuration();

            //Allow older deprecated schemas
            conf.setBoolean("parquet.avro.readInt96AsFixed", true);

            final InputFile inputFile = new NifiParquetInputFile(new ByteArrayInputStream(data), data.length);
            final ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build();

            //Format and write out each record
            GenericRecord record;
            boolean firstRecord = true;
            response.getOutputStream().write("[\n".getBytes());
            while ((record = reader.read()) != null) {
                if (firstRecord) {
                    firstRecord = false;
                } else {
                    response.getOutputStream().write(",\n".getBytes());
                }

                objectMapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), objectMapper.readTree(record.toString()));
            }
            response.getOutputStream().write("\n]".getBytes());
        } catch (final Throwable t) {
            logger.warn("Unable to format FlowFile content", t);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to format FlowFile content");
        }
    }

    private byte[] getInputStreamBytes(final InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[BUFFER_SIZE];
        long totalRead = 0;

        while (totalRead < MAX_CONTENT_SIZE) {
            int bytesRead = inputStream.read(buffer, 0, BUFFER_SIZE);
            if (bytesRead == -1) {
                break;
            }
            baos.write(buffer, 0, bytesRead);
            totalRead += bytesRead;
        }

        // Return empty array if inputStream has more data beyond maxBytes
        if (totalRead >= MAX_CONTENT_SIZE && inputStream.read() != -1) {
            return new byte[0];
        }

        return baos.toByteArray();
    }
}
