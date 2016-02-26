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
package org.apache.nifi.processors.media;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"media", "file", "format", "metadata", "audio", "video", "image", "document", "pdf"})
@CapabilityDescription("Extract the content metadata from flowfiles containing audio, video, image, and other file "
        + "types.  This processor relies on the Apache Tika project for file format detection and parsing.  It "
        + "extracts a long list of metadata types for media files including audio, video, and print media "
        + "formats."
        + "For the more details and the list of supported file types, visit the library's website "
        + "at http://tika.apache.org/.")
@WritesAttributes({@WritesAttribute(attribute = "<Metadata Key Prefix>.<attribute>", description = "The extracted content metadata "
        + "will be inserted with the attribute name \"<Metadata Key Prefix>.<attribute>\", or \"<attribute>\" if "
        + "\"Metadata Key Prefix\" is not provided.")})
@SupportsBatching
public class ExtractMediaMetadata extends AbstractProcessor {

    static final PropertyDescriptor MAX_NUMBER_OF_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Max Number of Attributes")
            .description("Specify the max number of attributes to add to the flowfile. There is no guarantee in what order"
                    + " the tags will be processed. By default it will process all of them.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor MAX_ATTRIBUTE_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Attribute Length")
            .description("Specifies the maximum length of a single attribute value.  When a metadata item has multiple"
                    + " values, they will be merged until this length is reached and then \", ...\" will be added as"
                    + " an indicator that additional values where dropped.  If a single value is longer than this, it"
                    + " will be truncated and \"(truncated)\" appended to indicate that truncation occurred.")
            .required(true)
            .defaultValue("10240")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MIME_TYPE_FILTER = new PropertyDescriptor.Builder()
            .name("MIME Type Filter")
            .description("A regular expression identifying MIME types for which metadata should extracted.  Flowfiles"
                    + " selected for scanning by the File Name Filter are parsed to determine the MIME type and extract"
                    + " metadata.  If the MIME type found matches this regular expression or this expression is"
                    + " blank, the metadata keys that match the Metadata Key Filter will be added to the flowfile"
                    + " attributes.  There is no guarantee in what order attributes will be produced.  If"
                    + " left blank, metadata will be extracted from all flow files selected for scanning.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor METADATA_KEY_FILTER = new PropertyDescriptor.Builder()
            .name("Metadata Key Filter")
            .description("A regular expression identifying which metadata keys received from the parser should be"
                    + " added to the flowfile attributes.  If left blank, all metadata keys parsed will be added to the"
                    + " flowfile attributes.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor METADATA_KEY_PREFIX = new PropertyDescriptor.Builder()
            .name("Metadata Key Prefix")
            .description("Text to be prefixed to metadata keys as the are added to the flowfile attributes.  It is"
                    + " recommended to end with with a separator character like '.' or '-', this is not automatically "
                    + " added by the processor.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor CONTENT_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Content Buffer Size")
            .description("The size for media content buffer during processing, or -1 for unlimited.  If not"
                    + " provided, the underlying parser default is used.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    Integer val = null;
                    try {
                        val = Integer.parseInt(input);
                    } catch (NumberFormatException ignore) {
                    }
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(val != null && val >= -1)
                            .explanation(subject + " must be a valid integer equal to or greater than -1.")
                            .build();
                }
            })
            .expressionLanguageSupported(true)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that successfully has media metadata extracted will be routed to success")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that fails to have media metadata extracted will be routed to failure")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<Pattern> mimeTypeFilterRef = new AtomicReference<>();
    private final AtomicReference<Pattern> metadataKeyFilterRef = new AtomicReference<>();

    private volatile AutoDetectParser autoDetectParser;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MAX_NUMBER_OF_ATTRIBUTES);
        properties.add(MAX_ATTRIBUTE_LENGTH);
        properties.add(MIME_TYPE_FILTER);
        properties.add(METADATA_KEY_FILTER);
        properties.add(METADATA_KEY_PREFIX);
        properties.add(CONTENT_BUFFER_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @SuppressWarnings("unused")
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final String mimeTypeFilterInput = context.getProperty(MIME_TYPE_FILTER).getValue();
        if (mimeTypeFilterInput != null && mimeTypeFilterInput.length() > 0) {
            mimeTypeFilterRef.set(Pattern.compile(mimeTypeFilterInput));
        } else {
            mimeTypeFilterRef.set(null);
        }

        String metadataKeyFilterInput = context.getProperty(METADATA_KEY_FILTER).getValue();
        if (metadataKeyFilterInput != null && metadataKeyFilterInput.length() > 0) {
            metadataKeyFilterRef.set(Pattern.compile(metadataKeyFilterInput));
        } else {
            metadataKeyFilterRef.set(null);
        }

        autoDetectParser = new AutoDetectParser();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = this.getLogger();
        final ObjectHolder<Map<String, String>> value = new ObjectHolder<>(null);
        final Integer maxAttribCount = context.getProperty(MAX_NUMBER_OF_ATTRIBUTES).asInteger();
        final Integer maxAttribLength = context.getProperty(MAX_ATTRIBUTE_LENGTH).asInteger();
        final Integer contentBufferSize = context.getProperty(CONTENT_BUFFER_SIZE).evaluateAttributeExpressions().asInteger();
        final String prefix = context.getProperty(METADATA_KEY_PREFIX).evaluateAttributeExpressions().getValue();

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try {
                        Map<String, String> results = tika_parse(in, prefix, maxAttribCount, maxAttribLength, contentBufferSize);
                        value.set(results);
                    } catch (SAXException | TikaException e) {
                        throw new IOException(e);
                    }
                }
            });

            // Write the results to attributes
            Map<String, String> results = value.get();
            if (results != null && !results.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, results);
            }

            session.transfer(flowFile, SUCCESS);
            session.getProvenanceReporter().modifyAttributes(flowFile, "media attributes extracted");
        } catch (ProcessException e) {
            logger.error("Failed to extract media metadata from {} due to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAILURE);
        }
    }

    private Map<String, String> tika_parse(InputStream sourceStream, String prefix, Integer maxAttribs, Integer maxAttribLen,
                                           Integer contentBufSize) throws IOException, TikaException, SAXException {
        final Metadata metadata = new Metadata();
        final TikaInputStream tikaInputStream = TikaInputStream.get(sourceStream);

        // as of June, 2016 the default BodyContentHandler used a 100,000 byte buffer
        final BodyContentHandler bodyContentHandler = (contentBufSize != null) ? new BodyContentHandler(contentBufSize) : new BodyContentHandler();
        autoDetectParser.parse(tikaInputStream, bodyContentHandler, metadata);
        final String content_type = metadata.get(Metadata.CONTENT_TYPE);

        // if parsed MIME type doesn't match filter fail fast without processing attributes
        final Pattern mimeTypeFilter = mimeTypeFilterRef.get();
        if (mimeTypeFilter != null && (content_type == null || !mimeTypeFilter.matcher(content_type).matches())) {
            return null;
        }

        final Map<String, String> results = new HashMap<>();
        final Pattern metadataKeyFilter = metadataKeyFilterRef.get();
        final StringBuilder dataBuilder = new StringBuilder();
        for (final String key : metadata.names()) {
            if (metadataKeyFilter != null && !metadataKeyFilter.matcher(key).matches()) {
                continue;
            }
            dataBuilder.setLength(0);
            if (metadata.isMultiValued(key)) {
                for (String val : metadata.getValues(key)) {
                    if (dataBuilder.length() > 1) {
                        dataBuilder.append(", ");
                    }
                    if (dataBuilder.length() + val.length() < maxAttribLen) {
                        dataBuilder.append(val);
                    } else {
                        dataBuilder.append("...");
                        break;
                    }
                }
            } else {
                dataBuilder.append(metadata.get(key));
            }
            if (prefix == null) {
                results.put(key, dataBuilder.toString().trim());
            } else {
                results.put(prefix + key, dataBuilder.toString().trim());
            }

            // cutoff at max if provided
            if (maxAttribs != null && results.size() >= maxAttribs) {
                break;
            }
        }
        return results;
    }
}
