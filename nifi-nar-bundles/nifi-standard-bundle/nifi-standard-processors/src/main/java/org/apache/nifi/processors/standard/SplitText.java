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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.util.IntegerHolder;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.ObjectHolder;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"split", "text"})
@InputRequirement(Requirement.INPUT_REQUIRED)
//@CapabilityDescription("Splits a text file into multiple smaller text files on line boundaries, each having up to a configured number of lines")
@CapabilityDescription("Splits a text file into multiple smaller text files on line boundaries or fragment size. " +
        "Each output file will contain configured number of lines or bytes. If both Line Split Count and Maximum Fragment Size " +
        "are specified, the split occurs at whichever limit is reached first. If a single line exceeds the Maximum Fragment Size, " +
        "that line will be output in a single split file which exceeds the configured maximum size limit. " +
        "NOTE: 'Remove Trailing Newlines' Property is deprecated; it may be removed in future releases.")
@WritesAttributes({
    @WritesAttribute(attribute = "text.line.count", description = "The number of lines of text from the original FlowFile that were copied to this FlowFile"),
    @WritesAttribute(attribute = "fragment.size", description = "The number of bytes from the original FlowFile that were copied to this FlowFile, " +
            "including header, if applicable, which is duplicated in each split FlowFile"),
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")})
@SeeAlso(MergeContent.class)
public class SplitText extends AbstractProcessor {

    // attribute keys
    public static final String SPLIT_LINE_COUNT = "text.line.count";
    public static final String FRAGMENT_SIZE = "fragment.size";
    public static final String FRAGMENT_ID = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";

    public static final PropertyDescriptor LINE_SPLIT_COUNT = new PropertyDescriptor.Builder()
            .name("Line Split Count")
            .description("The number of lines that will be added to each split file, excluding header lines. " +
                    "A value of zero requires Maximum Fragment Size to be set, and line count will not be considered in determining splits.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor FRAGMENT_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Fragment Size")
            .description("The maximum size of each split file, including header lines. NOTE: in the case where a " +
                    "single line exceeds this property (including headers, if applicable), that line will be output " +
                    "in a split of its own which exceeds this Maximum Fragment Size setting.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor HEADER_LINE_COUNT = new PropertyDescriptor.Builder()
            .name("Header Line Count")
            .description("The number of lines that should be considered part of the header; the header lines will be duplicated to all split files")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();
    public static final PropertyDescriptor HEADER_MARKER = new PropertyDescriptor.Builder()
            .name("Header Line Marker Characters")
            .description("The first character(s) on the line of the datafile which signifies a header line. This value is ignored when Header Line Count is non-zero.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor REMOVE_TRAILING_NEWLINES = new PropertyDescriptor.Builder()
            .name("Remove Trailing Newlines")
            .description("Whether to remove newlines at the end of each split file. This should be false if you intend to merge the split files later. " +
                    "\nDEPRECATED: This Property no longer has any effect and may be removed in future releases.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination when it has been successfully split into 1 or more files")
            .build();
    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("The split files will be routed to this destination when an input file is successfully split into 1 or more split files")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a file cannot be split for some reason, the original file will be routed to this destination and nothing will be routed elsewhere")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LINE_SPLIT_COUNT);
        properties.add(FRAGMENT_MAX_SIZE);
        properties.add(HEADER_LINE_COUNT);
        properties.add(HEADER_MARKER);
        properties.add(REMOVE_TRAILING_NEWLINES);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLITS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        ArrayList<ValidationResult> results = new ArrayList<>();

        results.add(new ValidationResult.Builder()
            .subject("Remove Trailing Newlines")
            .valid(!validationContext.getProperty(REMOVE_TRAILING_NEWLINES).asBoolean())
            .explanation("Property is deprecated; value must be set to false. Future releases may remove this Property.")
            .build());

        final boolean invalidState = (validationContext.getProperty(LINE_SPLIT_COUNT).asInteger() == 0
                && validationContext.getProperty(FRAGMENT_MAX_SIZE).asDataSize(DataUnit.B) == null);

        results.add(new ValidationResult.Builder()
            .subject("Maximum Fragment Size")
            .valid(!invalidState)
            .explanation("Property must be specified when Line Split Count is 0")
            .build()
        );
        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private int readLines(final InputStream in, final int maxNumLines, final long maxByteCount, final OutputStream out) throws IOException {
        int numLines = 0;
        long totalBytes = 0L;
        for (int i = 0; i < maxNumLines; i++) {
            final long bytes = countBytesToSplitPoint(in, out, totalBytes, maxByteCount);
            totalBytes += bytes;
            if (bytes <= 0) {
                return numLines;
            }
            numLines++;
            if (totalBytes >= maxByteCount && numLines > maxNumLines) {
                break;
            }
        }
        return numLines;
    }

    private long countBytesToSplitPoint(final InputStream in, final OutputStream out, final long bytesReadSoFar, final long maxSize) throws IOException {
        long bytesRead = 0L;
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        in.mark(Integer.MAX_VALUE);
        while (true) {
            final int nextByte = in.read();

            // if we hit end of stream we're done
            if (nextByte == -1) {
                if (out != null) {
                    buffer.writeTo(out);
                }
                buffer.close();
                return bytesRead;
            }

            // buffer the output
            bytesRead++;
            buffer.write(nextByte);

            // check the size limit
            if (bytesRead > (maxSize-bytesReadSoFar) && bytesReadSoFar > 0) {
                in.reset();
                buffer.close();
                return -1;
            }

            // if we have a new line, then we're done
            if (nextByte == '\n') {
                if (out != null) {
                    buffer.writeTo(out);
                }
                buffer.close();
                return bytesRead;
            }

            // Determine if \n follows \r; for both cases, end of line has been reached
            if (nextByte == '\r') {
                buffer.writeTo(out);
                buffer.close();
                in.mark(1);
                final int lookAheadByte = in.read();
                if (lookAheadByte == '\n') {
                    out.write(lookAheadByte);
                    return bytesRead + 1;
                } else {
                    in.reset();
                    return bytesRead;
                }
            }
        }
    }

    private SplitInfo countBytesToSplitPoint(final InputStream in, final int maxLines, long maxSize) throws IOException {
        SplitInfo info = new SplitInfo();

        while (info.lengthLines < maxLines) {
            final long bytesTillNext = countBytesToSplitPoint(in, null, info.lengthBytes, maxSize);
            if (bytesTillNext <= 0L) {
                break;
            }

            info.lengthLines++;
            info.lengthBytes += bytesTillNext;
        }

        return info;
    }

    private int countHeaderLines(final ByteCountingInputStream in,
            final String headerMarker) throws IOException {
        int headerInfo = 0;

        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        in.mark(Integer.MAX_VALUE);
        String line = br.readLine();
        while (line != null) {
            // if line is not a header line, reset stream and return header counts
            if (!line.startsWith(headerMarker) || line == null) {
                in.reset();
                return headerInfo;
            } else {
                headerInfo++;
            }
            line = br.readLine();
        }
        in.reset();
        return headerInfo;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final int headerCount = context.getProperty(HEADER_LINE_COUNT).asInteger();
        final int maxLineCount = (context.getProperty(LINE_SPLIT_COUNT).asInteger() == 0)
                ? Integer.MAX_VALUE : context.getProperty(LINE_SPLIT_COUNT).asInteger();
        final long maxFragmentSize = context.getProperty(FRAGMENT_MAX_SIZE).isSet()
                ? context.getProperty(FRAGMENT_MAX_SIZE).asDataSize(DataUnit.B).longValue() : Long.MAX_VALUE;
        final String headerMarker = context.getProperty(HEADER_MARKER).getValue();

        final ObjectHolder<String> errorMessage = new ObjectHolder<>(null);
        final ArrayList<SplitInfo> splitInfos = new ArrayList<>();

        final long startNanos = System.nanoTime();
        final List<FlowFile> splits = new ArrayList<>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                try (final BufferedInputStream bufferedIn = new BufferedInputStream(rawIn);
                        final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                    // if we have header lines, copy them into a ByteArrayOutputStream
                    final ByteArrayOutputStream headerStream = new ByteArrayOutputStream();
                    // Determine the number of lines of header, priority given to HEADER_LINE_COUNT property
                    int headerInfoLineCount = 0;
                    if (headerCount > 0) {
                        headerInfoLineCount = headerCount;
                    } else {
                        if (headerMarker != null) {
                            headerInfoLineCount = countHeaderLines(in, headerMarker);
                        }
                    }
                    final int headerLinesCopied = readLines(in, headerInfoLineCount, Long.MAX_VALUE, headerStream);

                    if (headerLinesCopied < headerInfoLineCount) {
                        errorMessage.set("Header Line Count is set to " + headerInfoLineCount + " but file had only " + headerLinesCopied + " lines");
                        return;
                    }

                    while (true) {
                        if (headerInfoLineCount > 0) {
                            // if we have header lines, create a new FlowFile, copy the header lines to that file,
                            // and then start copying lines
                            final IntegerHolder linesCopied = new IntegerHolder(0);
                            final LongHolder bytesCopied = new LongHolder(0L);
                            FlowFile splitFile = session.create(flowFile);
                            try {
                                splitFile = session.write(splitFile, new OutputStreamCallback() {
                                    @Override
                                    public void process(final OutputStream rawOut) throws IOException {
                                        try (final BufferedOutputStream out = new BufferedOutputStream(rawOut);
                                                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(out)) {
                                            headerStream.writeTo(countingOut);
                                            //readLines has an offset of countingOut.getBytesWritten() to allow for header bytes written already
                                            linesCopied.set(readLines(in, maxLineCount, maxFragmentSize - countingOut.getBytesWritten(), countingOut));
                                            bytesCopied.set(countingOut.getBytesWritten());
                                        }
                                    }
                                });
                                splitFile = session.putAttribute(splitFile, SPLIT_LINE_COUNT, String.valueOf(linesCopied.get()));
                                splitFile = session.putAttribute(splitFile, FRAGMENT_SIZE, String.valueOf(bytesCopied.get()));
                                logger.debug("Created Split File {} with {} lines, {} bytes", new Object[]{splitFile, linesCopied.get(), bytesCopied.get()});
                            } finally {
                                if (linesCopied.get() > 0) {
                                    splits.add(splitFile);
                                } else {
                                    // if the number of content lines is a multiple of the SPLIT_LINE_COUNT,
                                    // the last flow file will contain just a header; don't forward that one
                                    session.remove(splitFile);
                                }
                            }

                            // Check for EOF
                            in.mark(1);
                            if (in.read() == -1) {
                                break;
                            }
                            in.reset();

                        } else {
                            // We have no header lines, so we can simply demarcate the original File via the
                            // ProcessSession#clone method.
                            long beforeReadingLines = in.getBytesConsumed();
                            final SplitInfo info = countBytesToSplitPoint(in, maxLineCount, maxFragmentSize);
                            if (info.lengthBytes == 0) {
                                // stream is out of data
                                break;
                            } else {
                                info.offsetBytes = beforeReadingLines;
                                splitInfos.add(info);
                                final long procNanos = System.nanoTime() - startNanos;
                                final long procMillis = TimeUnit.MILLISECONDS.convert(procNanos, TimeUnit.NANOSECONDS);
                                logger.debug("Detected start of Split File in {} at byte offset {} with a length of {} bytes; "
                                        + "total splits = {}; total processing time = {} ms",
                                        new Object[]{flowFile, beforeReadingLines, info.lengthBytes, splitInfos.size(), procMillis});
                            }
                        }
                    }
                }
            }
        });

        if (errorMessage.get() != null) {
            logger.error("Unable to split {} due to {}; routing to failure", new Object[]{flowFile, errorMessage.get()});
            session.transfer(flowFile, REL_FAILURE);
            if (splits != null && !splits.isEmpty()) {
                session.remove(splits);
            }
            return;
        }

        if (!splitInfos.isEmpty()) {
            // Create the splits
            for (final SplitInfo info : splitInfos) {
                FlowFile split = session.clone(flowFile, info.offsetBytes, info.lengthBytes);
                split = session.putAttribute(split, SPLIT_LINE_COUNT, String.valueOf(info.lengthLines));
                split = session.putAttribute(split, FRAGMENT_SIZE, String.valueOf(info.lengthBytes));
                splits.add(split);
            }
        }
        finishFragmentAttributes(session, flowFile, splits);

        if (splits.size() > 10) {
            logger.info("Split {} into {} files", new Object[]{flowFile, splits.size()});
        } else {
            logger.info("Split {} into {} files: {}", new Object[]{flowFile, splits.size(), splits});
        }

        session.transfer(flowFile, REL_ORIGINAL);
        session.transfer(splits, REL_SPLITS);
    }

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

    private static class SplitInfo {

        public long offsetBytes;
        public long lengthBytes;
        public long lengthLines;

        public SplitInfo() {
            super();
            this.offsetBytes = 0L;
            this.lengthBytes = 0L;
            this.lengthLines = 0L;
        }

        @SuppressWarnings("unused")
        public SplitInfo(long offsetBytes, long lengthBytes, long lengthLines) {
            super();
            this.offsetBytes = offsetBytes;
            this.lengthBytes = lengthBytes;
            this.lengthLines = lengthLines;
        }
    }

}
