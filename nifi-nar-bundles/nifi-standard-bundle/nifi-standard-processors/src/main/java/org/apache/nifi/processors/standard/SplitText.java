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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.nifi.logging.ComponentLog;
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

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"split", "text"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a text file into multiple smaller text files on line boundaries limited by maximum number of lines "
        + "or total size of fragment. Each output split file will contain no more than the configured number of lines or bytes. "
        + "If both Line Split Count and Maximum Fragment Size are specified, the split occurs at whichever limit is reached first. "
        + "If the first line of a fragment exceeds the Maximum Fragment Size, that line will be output in a single split file which "
        +" exceeds the configured maximum size limit.")
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
            .description("The first character(s) on the line of the datafile which signifies a header line. This value is ignored when Header Line Count is non-zero. " +
                    "The first line not containing the Header Line Marker Characters and all subsequent lines are considered non-header")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor REMOVE_TRAILING_NEWLINES = new PropertyDescriptor.Builder()
            .name("Remove Trailing Newlines")
            .description("Whether to remove newlines at the end of each split file. This should be false if you intend to merge the split files later. If this is set to "
                    + "'true' and a FlowFile is generated that contains only 'empty lines' (i.e., consists only of \r and \n characters), the FlowFile will not be emitted. "
                    + "Note, however, that if header lines are specified, the resultant FlowFile will never be empty as it will consist of the header lines, so "
                    + "a FlowFile may be emitted that contains only the header lines.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
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
        List<ValidationResult> results = new ArrayList<>();

        final boolean invalidState = (validationContext.getProperty(LINE_SPLIT_COUNT).asInteger() == 0
                && !validationContext.getProperty(FRAGMENT_MAX_SIZE).isSet());

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

    private int readLines(final InputStream in, final int maxNumLines, final long maxByteCount, final OutputStream out,
                          final boolean includeLineDelimiter, final byte[] leadingNewLineBytes) throws IOException {
        final EndOfLineBuffer eolBuffer = new EndOfLineBuffer();

        byte[] leadingBytes = leadingNewLineBytes;
        int numLines = 0;
        long totalBytes = 0L;
        for (int i = 0; i < maxNumLines; i++) {
            final EndOfLineMarker eolMarker = countBytesToSplitPoint(in, out, totalBytes, maxByteCount, includeLineDelimiter, eolBuffer, leadingBytes);
            final long bytes = eolMarker.getBytesConsumed();
            leadingBytes = eolMarker.getLeadingNewLineBytes();

            if (includeLineDelimiter && out != null) {
                if (leadingBytes != null) {
                    out.write(leadingBytes);
                    leadingBytes = null;
                }
                eolBuffer.drainTo(out);
            }
            totalBytes += bytes;
            if (bytes <= 0) {
                return numLines;
            }
            numLines++;
            if (totalBytes >= maxByteCount) {
                break;
            }
        }
        return numLines;
    }

    private EndOfLineMarker countBytesToSplitPoint(final InputStream in, final OutputStream out, final long bytesReadSoFar, final long maxSize,
                                                   final boolean includeLineDelimiter, final EndOfLineBuffer eolBuffer, final byte[] leadingNewLineBytes) throws IOException {
        long bytesRead = 0L;
        final ByteArrayOutputStream buffer;
        if (out != null) {
            buffer = new ByteArrayOutputStream();
        } else {
            buffer = null;
        }
        byte[] bytesToWriteFirst = leadingNewLineBytes;

        in.mark(Integer.MAX_VALUE);
        while (true) {
            final int nextByte = in.read();

            // if we hit end of stream we're done
            if (nextByte == -1) {
                if (buffer != null) {
                    buffer.writeTo(out);
                    buffer.close();
                }
                return new EndOfLineMarker(bytesRead, eolBuffer, true, bytesToWriteFirst);  // bytesToWriteFirst should be "null"?
            }

            // Verify leading bytes do not violate size limitation
            if (bytesToWriteFirst != null && (bytesToWriteFirst.length + bytesRead) > (maxSize - bytesReadSoFar) && includeLineDelimiter) {
                return new EndOfLineMarker(-1, eolBuffer, false, leadingNewLineBytes);
            }
            // Write leadingNewLines, if appropriate
            if ( buffer != null && includeLineDelimiter && bytesToWriteFirst != null) {
                bytesRead += bytesToWriteFirst.length;
                buffer.write(bytesToWriteFirst);
                bytesToWriteFirst = null;
            }
            // buffer the output
            bytesRead++;
            if (buffer != null && nextByte != '\n' && nextByte != '\r') {
                if (bytesToWriteFirst != null) {
                    buffer.write(bytesToWriteFirst);
                }
                bytesToWriteFirst = null;
                eolBuffer.drainTo(buffer);
                eolBuffer.clear();
                buffer.write(nextByte);
            }

            // check the size limit
            if (bytesRead > (maxSize-bytesReadSoFar) && bytesReadSoFar > 0) {
                in.reset();
                if (buffer != null) {
                    buffer.close();
                }
                return new EndOfLineMarker(-1, eolBuffer, false, leadingNewLineBytes);
            }

            // if we have a new line, then we're done
            if (nextByte == '\n') {
                if (buffer != null) {
                    buffer.writeTo(out);
                    buffer.close();
                    eolBuffer.addEndOfLine(false, true);
                }
                return new EndOfLineMarker(bytesRead, eolBuffer, false, bytesToWriteFirst);
            }

            // Determine if \n follows \r; in either case, end of line has been reached
            if (nextByte == '\r') {
                if (buffer != null) {
                    buffer.writeTo(out);
                    buffer.close();
                }
                in.mark(1);
                final int lookAheadByte = in.read();
                if (lookAheadByte == '\n') {
                    eolBuffer.addEndOfLine(true, true);
                    return new EndOfLineMarker(bytesRead + 1, eolBuffer, false, bytesToWriteFirst);
                } else {
                    in.reset();
                    eolBuffer.addEndOfLine(true, false);
                    return new EndOfLineMarker(bytesRead, eolBuffer, false, bytesToWriteFirst);
                }
            }
        }
    }

    private SplitInfo locateSplitPoint(final InputStream in, final int numLines, final boolean keepAllNewLines, final long maxSize,
                                       final long bufferedBytes) throws IOException {
        final SplitInfo info = new SplitInfo();
        final EndOfLineBuffer eolBuffer = new EndOfLineBuffer();
        int lastByte = -1;
        info.lengthBytes = bufferedBytes;
        long lastEolBufferLength = 0L;

        while ((info.lengthLines < numLines || (info.lengthLines == numLines && lastByte == '\r'))
                && (((info.lengthBytes + eolBuffer.length()) < maxSize) || info.lengthLines == 0)
                && eolBuffer.length() < maxSize) {
            in.mark(1);
            final int nextByte = in.read();
            // Check for \n following \r on last line
            if (info.lengthLines == numLines && lastByte == '\r' && nextByte != '\n') {
                in.reset();
                break;
            }
            switch (nextByte) {
                case -1:
                    info.endOfStream = true;
                    if (keepAllNewLines) {
                        info.lengthBytes += eolBuffer.length();
                    }
                    if (lastByte != '\r') {
                        info.lengthLines++;
                    }
                    info.bufferedBytes = 0;
                    return info;
                case '\r':
                    eolBuffer.addEndOfLine(true, false);
                    info.lengthLines++;
                    info.bufferedBytes = 0;
                    break;
                case '\n':
                    eolBuffer.addEndOfLine(false, true);
                    if (lastByte != '\r') {
                        info.lengthLines++;
                    }
                    info.bufferedBytes = 0;
                    break;
                default:
                    if (eolBuffer.length() > 0) {
                        info.lengthBytes += eolBuffer.length();
                        lastEolBufferLength = eolBuffer.length();
                        eolBuffer.clear();
                    }
                    info.lengthBytes++;
                    info.bufferedBytes++;
                    break;
            }
            lastByte = nextByte;
        }
        // if current line exceeds size and not keeping eol characters, remove previously applied eol characters
        if ((info.lengthBytes + eolBuffer.length()) >= maxSize && !keepAllNewLines) {
            info.lengthBytes -= lastEolBufferLength;
        }
        if (keepAllNewLines) {
            info.lengthBytes += eolBuffer.length();
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
            if (!line.startsWith(headerMarker)) {
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

        final ComponentLog logger = getLogger();
        final int headerCount = context.getProperty(HEADER_LINE_COUNT).asInteger();
        final int maxLineCount = (context.getProperty(LINE_SPLIT_COUNT).asInteger() == 0)
                ? Integer.MAX_VALUE : context.getProperty(LINE_SPLIT_COUNT).asInteger();
        final long maxFragmentSize = context.getProperty(FRAGMENT_MAX_SIZE).isSet()
                ? context.getProperty(FRAGMENT_MAX_SIZE).asDataSize(DataUnit.B).longValue() : Long.MAX_VALUE;
        final String headerMarker = context.getProperty(HEADER_MARKER).getValue();
        final boolean includeLineDelimiter = !context.getProperty(REMOVE_TRAILING_NEWLINES).asBoolean();

        final AtomicReference<String> errorMessage = new AtomicReference<>(null);
        final ArrayList<SplitInfo> splitInfos = new ArrayList<>();

        final long startNanos = System.nanoTime();
        final List<FlowFile> splits = new ArrayList<>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                try (final BufferedInputStream bufferedIn = new BufferedInputStream(rawIn);
                        final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                    long bufferedPartialLine = 0;

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

                    final byte[] headerNewLineBytes;
                    final byte[] headerBytesWithoutTrailingNewLines;
                    if (headerInfoLineCount > 0) {
                        final int headerLinesCopied = readLines(in, headerInfoLineCount, Long.MAX_VALUE, headerStream, true, null);

                        if (headerLinesCopied < headerInfoLineCount) {
                            errorMessage.set("Header Line Count is set to " + headerInfoLineCount + " but file had only " + headerLinesCopied + " lines");
                            return;
                        }

                        // Break header apart into trailing newlines and remaining text
                        final byte[] headerBytes = headerStream.toByteArray();
                        int headerNewLineByteCount = 0;
                        for (int i = headerBytes.length - 1; i >= 0; i--) {
                            final byte headerByte = headerBytes[i];

                            if (headerByte == '\r' || headerByte == '\n') {
                                headerNewLineByteCount++;
                            } else {
                                break;
                            }
                        }

                        if (headerNewLineByteCount == 0) {
                            headerNewLineBytes = null;
                            headerBytesWithoutTrailingNewLines = headerBytes;
                        } else {
                            headerNewLineBytes = new byte[headerNewLineByteCount];
                            System.arraycopy(headerBytes, headerBytes.length - headerNewLineByteCount, headerNewLineBytes, 0, headerNewLineByteCount);

                            headerBytesWithoutTrailingNewLines = new byte[headerBytes.length - headerNewLineByteCount];
                            System.arraycopy(headerBytes, 0, headerBytesWithoutTrailingNewLines, 0, headerBytes.length - headerNewLineByteCount);
                        }
                    } else {
                        headerBytesWithoutTrailingNewLines = null;
                        headerNewLineBytes = null;
                    }

                    while (true) {
                        if (headerInfoLineCount > 0) {
                            // if we have header lines, create a new FlowFile, copy the header lines to that file,
                            // and then start copying lines
                            final AtomicInteger linesCopied = new AtomicInteger(0);
                            final AtomicLong bytesCopied = new AtomicLong(0L);
                            FlowFile splitFile = session.create(flowFile);
                            try {
                                splitFile = session.write(splitFile, new OutputStreamCallback() {
                                    @Override
                                    public void process(final OutputStream rawOut) throws IOException {
                                        try (final BufferedOutputStream out = new BufferedOutputStream(rawOut);
                                                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(out)) {
                                            countingOut.write(headerBytesWithoutTrailingNewLines);
                                            //readLines has an offset of countingOut.getBytesWritten() to allow for header bytes written already
                                            linesCopied.set(readLines(in, maxLineCount, maxFragmentSize - countingOut.getBytesWritten(), countingOut,
                                                    includeLineDelimiter, headerNewLineBytes));
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
                            long beforeReadingLines = in.getBytesConsumed() - bufferedPartialLine;
                            final SplitInfo info = locateSplitPoint(in, maxLineCount, includeLineDelimiter, maxFragmentSize, bufferedPartialLine);
                            if (context.getProperty(FRAGMENT_MAX_SIZE).isSet()) {
                                bufferedPartialLine = info.bufferedBytes;
                            }
                            if (info.endOfStream) {
                                // stream is out of data
                                if (info.lengthBytes > 0) {
                                    info.offsetBytes = beforeReadingLines;
                                    splitInfos.add(info);
                                    final long procNanos = System.nanoTime() - startNanos;
                                    final long procMillis = TimeUnit.MILLISECONDS.convert(procNanos, TimeUnit.NANOSECONDS);
                                    logger.debug("Detected start of Split File in {} at byte offset {} with a length of {} bytes; "
                                                    + "total splits = {}; total processing time = {} ms",
                                            new Object[]{flowFile, beforeReadingLines, info.lengthBytes, splitInfos.size(), procMillis});
                                }
                                break;
                            } else {
                                if (info.lengthBytes != 0) {
                                    info.offsetBytes = beforeReadingLines;
                                    info.lengthBytes -= bufferedPartialLine;
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
            }
        });

        if (errorMessage.get() != null) {
            logger.error("Unable to split {} due to {}; routing to failure", new Object[]{flowFile, errorMessage.get()});
            session.transfer(flowFile, REL_FAILURE);
            if (!splits.isEmpty()) {
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
        public long bufferedBytes;
        public boolean endOfStream;

        public SplitInfo() {
            this.offsetBytes = 0L;
            this.lengthBytes = 0L;
            this.lengthLines = 0L;
            this.bufferedBytes = 0L;
            this.endOfStream = false;
        }
    }

    public static class EndOfLineBuffer {
        private static final byte CARRIAGE_RETURN = (byte) '\r';
        private static final byte NEWLINE = (byte) '\n';

        private final BitSet buffer = new BitSet();
        private int index = 0;

        public void clear() {
            index = 0;
        }

        public void addEndOfLine(final boolean carriageReturn, final boolean newLine) {
            buffer.set(index++, carriageReturn);
            buffer.set(index++, newLine);
        }

        private void drainTo(final OutputStream out) throws IOException {
            for (int i = 0; i < index; i += 2) {
                final boolean cr = buffer.get(i);
                final boolean nl = buffer.get(i + 1);

                // we've consumed all data in the buffer
                if (!cr && !nl) {
                    return;
                }

                if (cr) {
                    out.write(CARRIAGE_RETURN);
                }

                if (nl) {
                    out.write(NEWLINE);
                }
            }

            clear();
        }

        /**
         * @return the number of line endings in the buffer
         */
        public int length() {
            return index / 2;
        }
    }

    public static class EndOfLineMarker {
        private final long bytesConsumed;
        private final EndOfLineBuffer eolBuffer;
        private final boolean streamEnded;
        private final byte[] leadingNewLineBytes;

        public EndOfLineMarker(final long bytesCounted, final EndOfLineBuffer eolBuffer, final boolean streamEnded, final byte[] leadingNewLineBytes) {
            this.bytesConsumed = bytesCounted;
            this.eolBuffer = eolBuffer;
            this.streamEnded = streamEnded;
            this.leadingNewLineBytes = leadingNewLineBytes;
        }

        public long getBytesConsumed() {
            return bytesConsumed;
        }

        public EndOfLineBuffer getEndOfLineBuffer() {
            return eolBuffer;
        }

        public boolean isStreamEnded() {
            return streamEnded;
        }

        public byte[] getLeadingNewLineBytes() {
            return leadingNewLineBytes;
        }
    }
}
