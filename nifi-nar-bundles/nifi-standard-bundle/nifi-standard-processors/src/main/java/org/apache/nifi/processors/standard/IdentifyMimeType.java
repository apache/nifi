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
package org.apache.nifi.processors.standard;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;

/**
 * <p>
 * Attempts to detect the MIME Type of a FlowFile by examining its contents. If the MIME Type is determined, it is added
 * to an attribute with the name mime.type. In addition, mime.extension is set if a common file extension is known.
 * </p>
 *
 * <p>
 * MIME Type detection is performed by Apache Tika; more information about detection is available at http://tika.apache.org.
 *
 * <ul>
 * <li>application/flowfile-v3</li>
 * <li>application/flowfile-v1</li>
 * </ul>
 * </p>
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"compression", "gzip", "bzip2", "zip", "MIME", "mime.type", "file", "identify"})
@CapabilityDescription("Attempts to identify the MIME Type used for a FlowFile. If the MIME Type can be identified, "
        + "an attribute with the name 'mime.type' is added with the value being the MIME Type. If the MIME Type cannot be determined, "
        + "the value will be set to 'application/octet-stream'. In addition, the attribute mime.extension will be set if a common file "
        + "extension for the MIME Type is known.")
@WritesAttribute(attribute = "mime.type", description = "This Processor sets the FlowFile's mime.type attribute to the detected MIME Type. "
        + "If unable to detect the MIME Type, the attribute's value will be set to application/octet-stream")
public class IdentifyMimeType extends AbstractProcessor {

    public static final PropertyDescriptor USE_FILENAME_IN_DETECTION = new PropertyDescriptor.Builder()
           .displayName("Use Filename In Detection")
           .name("use-filename-in-detection")
           .description("If true will pass the filename to Tika to aid in detection.")
           .required(true)
           .allowableValues("true", "false")
           .defaultValue("true")
           .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final TikaConfig config;
    private final Detector detector;

    public IdentifyMimeType() {
        // Setup Tika
        this.config = TikaConfig.getDefaultConfig();
        this.detector = config.getDetector();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(USE_FILENAME_IN_DETECTION);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream stream) throws IOException {
                try (final InputStream in = new BufferedInputStream(stream)) {
                    TikaInputStream tikaStream = TikaInputStream.get(in);
                    Metadata metadata = new Metadata();

                    if (filename != null && context.getProperty(USE_FILENAME_IN_DETECTION).asBoolean()) {
                        metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, filename);
                    }
                    // Get mime type
                    MediaType mediatype = detector.detect(tikaStream, metadata);
                    mimeTypeRef.set(mediatype.toString());
                }
            }
        });

        String mimeType = mimeTypeRef.get();
        String extension = "";
        try {
            MimeType mimetype;
            mimetype = config.getMimeRepository().forName(mimeType);
            extension = mimetype.getExtension();
        } catch (MimeTypeException ex) {
            logger.warn("MIME type extension lookup failed: {}", new Object[]{ex});
        }

        // Workaround for bug in Tika - https://issues.apache.org/jira/browse/TIKA-1563
        if (mimeType != null && mimeType.equals("application/gzip") && extension.equals(".tgz")) {
            extension = ".gz";
        }

        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            flowFile = session.putAttribute(flowFile, "mime.extension", "");
            logger.info("Unable to identify MIME Type for {}; setting to application/octet-stream", new Object[]{flowFile});
        } else {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
            flowFile = session.putAttribute(flowFile, "mime.extension", extension);
            logger.info("Identified {} as having MIME Type {}", new Object[]{flowFile, mimeType});
        }

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }
}
