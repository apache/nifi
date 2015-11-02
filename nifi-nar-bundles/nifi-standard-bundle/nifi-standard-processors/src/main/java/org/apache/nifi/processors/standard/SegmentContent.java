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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"segment", "split"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Segments a FlowFile into multiple smaller segments on byte boundaries. Each segment is given the following attributes: "
        + "fragment.identifier, fragment.index, fragment.count, segment.original.filename; these attributes can then be used by the "
        + "MergeContent processor in order to reconstitute the original FlowFile")
@WritesAttributes({
    @WritesAttribute(attribute = "segment.identifier",
            description = "All segments produced from the same parent FlowFile will have the same randomly generated UUID added for this "
            + "attribute. This attribute is added to maintain backward compatibility, but the fragment.identifier is preferred, as "
            + "it is designed to work in conjunction with the MergeContent Processor"),
    @WritesAttribute(attribute = "segment.index",
            description = "A one-up number that indicates the ordering of the segments that were created from a single parent FlowFile. "
            + "This attribute is added to maintain backward compatibility, but the fragment.index is preferred, as it is designed "
            + "to work in conjunction with the MergeContent Processor"),
    @WritesAttribute(attribute = "segment.count",
            description = "The number of segments generated from the parent FlowFile. This attribute is added to maintain backward compatibility, "
            + "but the fragment.count is preferred, as it is designed to work in conjunction with the MergeContent Processor"),
    @WritesAttribute(attribute = "fragment.identifier",
            description = "All segments produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index",
            description = "A one-up number that indicates the ordering of the segments that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of segments generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ",
            description = "The filename will be updated to include the parent's filename, the segment index, and the segment count")})
@SeeAlso(MergeContent.class)
public class SegmentContent extends AbstractProcessor {

    public static final String SEGMENT_ID = "segment.identifier";
    public static final String SEGMENT_INDEX = "segment.index";
    public static final String SEGMENT_COUNT = "segment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";

    public static final String FRAGMENT_ID = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";

    public static final PropertyDescriptor SIZE = new PropertyDescriptor.Builder()
            .name("Segment Size")
            .description("The maximum data size for each segment")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .required(true)
            .build();

    public static final Relationship REL_SEGMENTS = new Relationship.Builder()
            .name("segments")
            .description("All segments will be sent to this relationship. If the file was small enough that it was not segmented, "
                    + "a copy of the original is sent to this relationship as well as original")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile will be sent to this relationship")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> propertyDescriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SEGMENTS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SIZE);
        this.propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String segmentId = UUID.randomUUID().toString();
        final long segmentSize = context.getProperty(SIZE).asDataSize(DataUnit.B).longValue();

        final String originalFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        if (flowFile.getSize() <= segmentSize) {
            flowFile = session.putAttribute(flowFile, SEGMENT_ID, segmentId);
            flowFile = session.putAttribute(flowFile, SEGMENT_INDEX, "1");
            flowFile = session.putAttribute(flowFile, SEGMENT_COUNT, "1");
            flowFile = session.putAttribute(flowFile, SEGMENT_ORIGINAL_FILENAME, originalFileName);

            flowFile = session.putAttribute(flowFile, FRAGMENT_ID, segmentId);
            flowFile = session.putAttribute(flowFile, FRAGMENT_INDEX, "1");
            flowFile = session.putAttribute(flowFile, FRAGMENT_COUNT, "1");

            FlowFile clone = session.clone(flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(clone, REL_SEGMENTS);
            return;
        }

        int totalSegments = (int) (flowFile.getSize() / segmentSize);
        if (totalSegments * segmentSize < flowFile.getSize()) {
            totalSegments++;
        }

        final Map<String, String> segmentAttributes = new HashMap<>();
        segmentAttributes.put(SEGMENT_ID, segmentId);
        segmentAttributes.put(SEGMENT_COUNT, String.valueOf(totalSegments));
        segmentAttributes.put(SEGMENT_ORIGINAL_FILENAME, originalFileName);

        segmentAttributes.put(FRAGMENT_ID, segmentId);
        segmentAttributes.put(FRAGMENT_COUNT, String.valueOf(totalSegments));

        final Set<FlowFile> segmentSet = new HashSet<>();
        for (int i = 1; i <= totalSegments; i++) {
            final long segmentOffset = segmentSize * (i - 1);
            FlowFile segment = session.clone(flowFile, segmentOffset, Math.min(segmentSize, flowFile.getSize() - segmentOffset));
            segmentAttributes.put(SEGMENT_INDEX, String.valueOf(i));
            segmentAttributes.put(FRAGMENT_INDEX, String.valueOf(i));
            segment = session.putAllAttributes(segment, segmentAttributes);
            segmentSet.add(segment);
        }

        session.transfer(segmentSet, REL_SEGMENTS);
        session.transfer(flowFile, REL_ORIGINAL);

        if (totalSegments <= 10) {
            getLogger().info("Segmented {} into {} segments: {}", new Object[]{flowFile, totalSegments, segmentSet});
        } else {
            getLogger().info("Segmented {} into {} segments", new Object[]{flowFile, totalSegments});
        }
    }
}
