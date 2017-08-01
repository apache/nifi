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
package org.apache.nifi.processors.rya;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "rya, ingest, triples" })
@CapabilityDescription("Provides triples ingesting into Rya.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class PutRya extends AbstractProcessor {

    public static final PropertyDescriptor RYA_API_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Rya API Endpoint")
            .defaultValue("http://server:8080/web.rya/loadrdf")
            .description("The Rya API endpoint for loading data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RYA_TRIPLES_FORMAT = new PropertyDescriptor.Builder()
            .name("Triples Format")
            .defaultValue("RDF/XML")
            .description("The format of the triples.")
            .allowableValues("RDF/XML", "N-Triples", "Turtle", "N3", "TriX", "TriG")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success").description("success").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure").description("failure").build();

    public static final MediaType MEDIA_TYPE_TEXT_PLAIN = MediaType.parse("text/plain");

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private final OkHttpClient client = new OkHttpClient();

    @Override
    protected void init(final ProcessorInitializationContext context) {

        descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(RYA_API_ENDPOINT);
        descriptors.add(RYA_TRIPLES_FORMAT);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();

        if (flowFile == null) {
        		context.yield();
            return;
        }

        final String ryaApiEndpoint = context.getProperty(RYA_API_ENDPOINT).getValue();
        final String ryaTriplesFormat = context.getProperty(RYA_TRIPLES_FORMAT).getValue();
        
        if(StringUtils.isBlank(ryaApiEndpoint)) {
        	     getLogger().debug("Rya API endpoint is required but was empty.");
             session.transfer(flowFile, REL_FAILURE);
             context.yield();
             return;
         }
        
        if(StringUtils.isBlank(ryaTriplesFormat)) {
        	     getLogger().debug("Triples format is required but was empty.");
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            return;
        }

        try {

            flowFile = session.write(flowFile, new StreamCallback() {

                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {

                    final String input = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                    sendToRya(ryaApiEndpoint, ryaTriplesFormat, input);

                }

            });

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception ex) {

            getLogger().error(String.format("Unable to send to Rya host: %s. Exception: %s", RYA_API_ENDPOINT, ex.getMessage()), ex);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            
        }

    }

    private void sendToRya(String url, String format, String triples) throws IOException {

        MediaType mediaType = MediaType.parse(getContentType(format));

        Request request = new Request.Builder()
                .url(url + "?format=" + format)
                .post(RequestBody.create(mediaType, triples))
                .build();

        Response response = client.newCall(request).execute();

        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }

    }

    private String getContentType(String format) {

        String contentType = "plain/text";

        if(StringUtils.equalsIgnoreCase(format, "RDF/XML")) {
            contentType = "rdf/xml";
        } else if(StringUtils.equalsIgnoreCase(format, "N-Triples")) {
            contentType = "plain/text";
        } else if(StringUtils.equalsIgnoreCase(format, "Turtle")) {
            contentType = "application/x-turtle";
        } else if(StringUtils.equalsIgnoreCase(format, "N3")) {
            contentType = "text/rdf+n3";
        } else if(StringUtils.equalsIgnoreCase(format, "TriX")) {
            contentType = "application/trix";
        } else if(StringUtils.equalsIgnoreCase(format, "TriG")) {
            contentType = "application/trig";
        }

        return contentType;

    }

}