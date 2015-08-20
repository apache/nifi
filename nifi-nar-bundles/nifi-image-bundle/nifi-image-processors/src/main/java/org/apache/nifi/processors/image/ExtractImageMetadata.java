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
package org.apache.nifi.processors.image;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

@Tags({"Exif", "Exchangeable", "image", "file", "format", "JPG", "GIF", "PNG", "BMP", "metadata","IPTC", "XMP"})
@CapabilityDescription("Extract the image metadata from flowfiles containing images. This processor relies on this "
        + "metadata extractor library https://github.com/drewnoakes/metadata-extractor. It extracts a long list of "
        + "metadata types including but not limited to EXIF, IPTC, XMP and Photoshop fields. For the full list visit "
        + "the library's website."
        + "NOTE: The library being used loads the images into memory so extremely large images may cause problems.")
@WritesAttributes({@WritesAttribute(attribute = "<directory name>.<tag name>", description = "The extracted image metadata "
        + "will be inserted with the attribute name \"<directory name>.<tag name>\". ")})
@SupportsBatching
public class ExtractImageMetadata extends AbstractProcessor {

    public static final PropertyDescriptor MaxAttributes = new PropertyDescriptor.Builder()
        .name("Max number of attributes")
        .description("Specify the max number of attributes to add to the flowfile. There is no guarantee in what order"
                + " the tags will be processed. By default it will process all of them.")
        .required(false)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that successfully has image metadata extracted will be routed to success")
        .build();

    public static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that fails to have image metadata extracted will be routed to failure")
        .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MaxAttributes);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = this.getLogger();
        FlowFile flowfile = session.get();

        if (flowfile == null) {
            return;
        }

        final ObjectHolder<Metadata> value = new ObjectHolder<>(null);
        String propertyValue = context.getProperty(MaxAttributes).getValue();
        final int max = propertyValue!=null ? Integer.parseInt(propertyValue) : -1;

        try {
            session.read(flowfile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try {
                        Metadata imageMetadata = ImageMetadataReader.readMetadata(in);
                        value.set(imageMetadata);
                    } catch (ImageProcessingException ex) {
                        throw new ProcessException(ex);
                    }
                }
            });

            Metadata metadata = value.get();
            Map<String, String> results = max == -1 ? getTags(metadata) : getTags(max, metadata);

            // Write the results to an attribute
            if (!results.isEmpty()) {
                flowfile = session.putAllAttributes(flowfile, results);
            }

            session.transfer(flowfile, SUCCESS);
        } catch (ProcessException e) {
            logger.error("Failed to extract image metadata from {} due to {}", new Object[]{flowfile, e});
            session.transfer(flowfile, FAILURE);
        }
    }

    private Map<String, String> getTags(Metadata metadata) {
        Map<String, String> results = new HashMap<>();

        for (Directory directory : metadata.getDirectories()) {
            for (Tag tag : directory.getTags()) {
                results.put(directory.getName() + "." + tag.getTagName(), tag.getDescription());
            }
        }

        return results;
    }

    private Map<String, String> getTags(int max, Metadata metadata) {
        Map<String, String> results = new HashMap<>();
        int i =0;

        for (Directory directory : metadata.getDirectories()) {
            for (Tag tag : directory.getTags()) {
                results.put(directory.getName() + "." + tag.getTagName(), tag.getDescription());

                i++;
                if(i>=max) {
                    return results;
                }
            }
        }

        return results;
    }
}
