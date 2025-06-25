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

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
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
import org.apache.parquet.web.utils.NifiParquetContentViewerInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ParquetContentViewerController extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ParquetContentViewerController.class);

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

        final boolean formatted = Boolean.parseBoolean(request.getParameter("formatted"));
        if (!formatted) {
            final InputStream contentStream = downloadableContent.getContent();
            contentStream.transferTo(response.getOutputStream());
            return;
        }

        // allow the user to drive the data type but fall back to the content type if necessary
        String displayName = request.getParameter("mimeTypeDisplayName");
        if (displayName == null) {
            displayName = downloadableContent.getType();
        }

        if (displayName == null || !(displayName.equals("parquet") || displayName.equals("application/parquet"))) {
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unknown content type");
            return;
        }

        try {
            //Output will be in a fixed length table format.
            List<String> fieldNames = new ArrayList<>();
            List<ArrayList<String>> columns = new ArrayList<>();

            //Convert InputStream to a seekable InputStream
            byte[] data = IOUtils.toByteArray(downloadableContent.getContent());
            Configuration conf = new Configuration();
            conf.setBoolean("parquet.avro.readInt96AsFixed", true);

            final InputFile inputFile = new NifiParquetContentViewerInputFile(new ByteArrayInputStream(data), data.length);
            final ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withConf(conf)
                    .build();

            //Get each column per record and save it to corresponding column list
            GenericRecord record;
            boolean needHeader = true;
            while ((record = reader.read()) != null) {
                //Get the field names from the schema
                if (needHeader) {
                    record.getSchema().getFields().forEach(f -> {
                        fieldNames.add(f.name());
                        ArrayList<String> column = new ArrayList<>();
                        column.add(f.name());
                        columns.add(column);
                    });
                    needHeader = false;
                }

                for (int i = 0; i < fieldNames.size(); i++) {
                    Object value = record.get(fieldNames.get(i));
                    if (value != null) {
                        columns.get(i).add(value.toString());
                    } else {
                        columns.get(i).add("");
                    }
                }
            }

            //Get the maximum length for each column including the header.
            int[] maxLengths = new int[fieldNames.size()];
            for (int i = 0; i < columns.size(); i++) {
                maxLengths[i] = columns.get(i).stream()
                        .max(Comparator.comparingInt(String::length))
                        .orElse("")
                        .length();
            }

            //Print out each field with a fixed length
            for (int i = 0; i < columns.getFirst().size(); i++) {
                StringBuilder row = new StringBuilder();
                for (int j = 0; j < columns.size(); j++) {
                    row.append(String.format("%-" + (maxLengths[j] + 1) + "s", columns.get(j).get(i)));
                }
                response.getOutputStream().write((row + "\n").getBytes());
            }

        } catch (final Throwable t) {
            logger.warn("Unable to format FlowFile content", t);
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to format FlowFile content");
        }
    }
}
