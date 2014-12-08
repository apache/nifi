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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.io.BufferedInputStream;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.NaiveSearchRingBuffer;
import org.apache.nifi.util.Tuple;

import org.apache.commons.codec.binary.Hex;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"content", "split", "binary"})
@CapabilityDescription("Splits incoming FlowFiles by a specified byte sequence")
public class SplitContent extends AbstractProcessor {

    // attribute keys
    public static final String FRAGMENT_ID = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";

    public static final PropertyDescriptor BYTE_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Byte Sequence")
            .description("A hex representation of bytes to look for and upon which to split the source file into separate files")
            .addValidator(new HexStringPropertyValidator())
            .required(true)
            .build();
    public static final PropertyDescriptor KEEP_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Keep Byte Sequence")
            .description("Determines whether or not the Byte Sequence should be included at the end of each Split")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("All Splits will be routed to the splits relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original file")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<byte[]> byteSequence = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SPLITS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BYTE_SEQUENCE);
        properties.add(KEEP_SEQUENCE);
        this.properties = Collections.unmodifiableList(properties);
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
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(BYTE_SEQUENCE)) {
            try {
                this.byteSequence.set(Hex.decodeHex(newValue.toCharArray()));
            } catch (final Exception e) {
                this.byteSequence.set(null);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final boolean keepSequence = context.getProperty(KEEP_SEQUENCE).asBoolean();
        final byte[] byteSequence = this.byteSequence.get();
        if (byteSequence == null) {   // should never happen. But just in case...
            logger.error("{} Unable to obtain Byte Sequence", new Object[]{this});
            session.rollback();
            return;
        }

        final List<Tuple<Long, Long>> splits = new ArrayList<>();

        final NaiveSearchRingBuffer buffer = new NaiveSearchRingBuffer(byteSequence);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                long bytesRead = 0L;
                long startOffset = 0L;

                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    while (true) {
                        final int nextByte = in.read();
                        if (nextByte == -1) {
                            return;
                        }

                        bytesRead++;
                        boolean matched = buffer.addAndCompare((byte) (nextByte & 0xFF));
                        if (matched) {
                            final long splitLength;

                            if (keepSequence) {
                                splitLength = bytesRead - startOffset;
                            } else {
                                splitLength = bytesRead - startOffset - byteSequence.length;
                            }

                            splits.add(new Tuple<>(startOffset, splitLength));
                            startOffset = bytesRead;
                            buffer.clear();
                        }
                    }
                }
            }
        });

        long lastOffsetPlusSize = -1L;
        if (splits.isEmpty()) {
            FlowFile clone = session.clone(flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(clone, REL_SPLITS);
            logger.info("Found no match for {}; transferring original 'original' and transferring clone {} to 'splits'", new Object[]{flowFile, clone});
            return;
        }

        final ArrayList<FlowFile> splitList = new ArrayList<>();
        for (final Tuple<Long, Long> tuple : splits) {
            long offset = tuple.getKey();
            long size = tuple.getValue();
            if (size > 0) {
                FlowFile split = session.clone(flowFile, offset, size);
                splitList.add(split);
            }

            lastOffsetPlusSize = offset + size;
        }

        long finalSplitOffset = lastOffsetPlusSize;
        if (!keepSequence) {
            finalSplitOffset += byteSequence.length;
        }
        if (finalSplitOffset > -1L && finalSplitOffset < flowFile.getSize()) {
            FlowFile finalSplit = session.clone(flowFile, finalSplitOffset, flowFile.getSize() - finalSplitOffset);
            splitList.add(finalSplit);
        }

        finishFragmentAttributes(session, flowFile, splitList);
        session.transfer(splitList, REL_SPLITS);
        session.transfer(flowFile, REL_ORIGINAL);

        if (splitList.size() > 10) {
            logger.info("Split {} into {} files", new Object[]{flowFile, splitList.size()});
        } else {
            logger.info("Split {} into {} files: {}", new Object[]{flowFile, splitList.size(), splitList});
        }
    }

    /**
     * Apply split index, count and other attributes.
     *
     * @param session
     * @param source
     * @param unpacked
     */
    private void finishFragmentAttributes(final ProcessSession session, final FlowFile source, final List<FlowFile> splits) {
        final String originalFilename = source.getAttribute(CoreAttributes.FILENAME.key());

        final String fragmentId = UUID.randomUUID().toString();
        final ArrayList<FlowFile> newList = new ArrayList<>(splits);
        splits.clear();
        for (int i = 1; i <= newList.size(); i++) {
            FlowFile ff = newList.get(i - 1);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, String.valueOf(i));
            attributes.put(FRAGMENT_COUNT, String.valueOf(newList.size()));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            splits.add(newFF);
        }
    }

    static class HexStringPropertyValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                Hex.decodeHex(input.toCharArray());
                return new ValidationResult.Builder().valid(true).input(input).subject(subject).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().valid(false).explanation("Not a valid Hex String").input(input).subject(subject).build();
            }
        }
    }
}
