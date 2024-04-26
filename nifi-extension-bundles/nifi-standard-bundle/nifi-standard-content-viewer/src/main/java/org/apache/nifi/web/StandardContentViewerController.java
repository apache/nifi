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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.web.ViewableContent.DisplayMode;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

public class StandardContentViewerController extends HttpServlet {

    private static final Set<String> supportedMimeTypes = new HashSet<>();

    static {
        supportedMimeTypes.add("application/json");
        supportedMimeTypes.add("application/xml");
        supportedMimeTypes.add("text/xml");
        supportedMimeTypes.add("text/plain");
        supportedMimeTypes.add("text/csv");
        supportedMimeTypes.add("application/avro-binary");
        supportedMimeTypes.add("avro/binary");
        supportedMimeTypes.add("application/avro+binary");
        supportedMimeTypes.add("text/x-yaml");
        supportedMimeTypes.add("text/yaml");
        supportedMimeTypes.add("text/yml");
        supportedMimeTypes.add("application/x-yaml");
        supportedMimeTypes.add("application/x-yml");
        supportedMimeTypes.add("application/yaml");
        supportedMimeTypes.add("application/yml");
    }

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ViewableContent content = (ViewableContent) request.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);

        // handle json/xml specifically, treat others as plain text
        String contentType = content.getContentType();
        if (supportedMimeTypes.contains(contentType)) {
            final String formatted;

            // leave the content alone if specified
            if (DisplayMode.Original.equals(content.getDisplayMode())) {
                formatted = content.getContent();
            } else {
                if ("application/json".equals(contentType)) {
                    // format json
                    final ObjectMapper mapper = new ObjectMapper();
                    final Object objectJson = mapper.readValue(content.getContentStream(), Object.class);
                    formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJson);
                } else if ("application/xml".equals(contentType) || "text/xml".equals(contentType)) {
                    // format xml
                    final StringWriter writer = new StringWriter();

                    try {
                        final StreamSource source = new StreamSource(content.getContentStream());
                        final StreamResult result = new StreamResult(writer);

                        final StandardTransformProvider transformProvider = new StandardTransformProvider();
                        transformProvider.setIndent(true);

                        transformProvider.transform(source, result);
                    } catch (final ProcessingException te) {
                        throw new IOException("Unable to transform content as XML: " + te, te);
                    }

                    // get the transformed xml
                    formatted = writer.toString();
                } else if ("application/avro-binary".equals(contentType) || "avro/binary".equals(contentType) || "application/avro+binary".equals(contentType)) {
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
                    try (final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(content.getContentStream(), datumReader)) {
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
                    formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJson);

                    contentType = "application/json";
                } else if ("text/x-yaml".equals(contentType) || "text/yaml".equals(contentType) || "text/yml".equals(contentType)
                        || "application/x-yaml".equals(contentType) || "application/x-yml".equals(contentType)
                        || "application/yaml".equals(contentType) || "application/yml".equals(contentType)) {
                    Yaml yaml = new Yaml();
                    // Parse the YAML file
                    final Object yamlObject = yaml.load(content.getContentStream());
                    DumperOptions options = new DumperOptions();
                    options.setIndent(2);
                    options.setPrettyFlow(true);
                    // Fix below - additional configuration
                    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
                    Yaml output = new Yaml(options);
                    formatted = output.dump(yamlObject);

                } else {
                    // leave plain text alone when formatting
                    formatted = content.getContent();
                }
            }

            request.setAttribute("mode", contentType);
            request.setAttribute("content", formatted);
            // Render using JSP from this Servlet Context
            this.getServletContext().getRequestDispatcher("/WEB-INF/jsp/codemirror.jsp").include(request, response);
        } else {
            final PrintWriter out = response.getWriter();
            out.println("Unexpected content type: " + contentType);
        }
    }
}
