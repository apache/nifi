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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.LongHolder;

// note: it is important that this Processor is not marked as @SupportsBatching because the session commits must complete before persisting state locally; otherwise, data loss may occur
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"tail", "file", "log", "text", "source"})
@CapabilityDescription("\"Tails\" a file, ingesting data from the file as it is written to the file. The file is expected to be textual. Data is ingested only when a "
    + "new line is encountered (carriage return or new-line character or combination). If the file to tail is periodically \"rolled over\", as is generally the case "
    + "with log files, an optional Rolling Filename Pattern can be used to retrieve data from files that have rolled over, even if the rollover occurred while NiFi "
    + "was not running (provided that the data still exists upon restart of NiFi). It is generally advisable to set the Run Schedule to a few seconds, rather than running "
    + "with the default value of 0 secs, as this Processor will consume a lot of resources if scheduled very aggressively. At this time, this Processor does not support "
    + "ingesting files that have been compressed when 'rolled over'.")
public class TailFile extends AbstractProcessor {

    static final AllowableValue START_BEGINNING_OF_TIME = new AllowableValue("Beginning of Time", "Beginning of Time",
        "Start with the oldest data that matches the Rolling Filename Pattern and then begin reading from the File to Tail");
    static final AllowableValue START_CURRENT_FILE = new AllowableValue("Beginning of File", "Beginning of File",
        "Start with the beginning of the File to Tail. Do not ingest any data that has already been rolled over");
    static final AllowableValue START_CURRENT_TIME = new AllowableValue("Current Time", "Current Time",
        "Start with the data at the end of the File to Tail. Do not ingest any data thas has already been rolled over or any data in the File to Tail that has already been written.");

    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
        .name("File to Tail")
        .description("Fully-qualified filename of the file that should be tailed")
        .expressionLanguageSupported(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();
    static final PropertyDescriptor ROLLING_FILENAME_PATTERN = new PropertyDescriptor.Builder()
        .name("Rolling Filename Pattern")
        .description("If the file to tail \"rolls over\" as would be the case with log files, this filename pattern will be used to "
            + "identify files that have rolled over so that if NiFi is restarted, and the file has rolled over, it will be able to pick up where it left off. "
            + "This pattern supports wildcard characters * and ? and will assume that the files that have rolled over live in the same directory as the file being tailed.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(false)
        .build();
    static final PropertyDescriptor STATE_FILE = new PropertyDescriptor.Builder()
        .name("State File")
        .description("Specifies the file that should be used for storing state about what data has been ingested so that upon restart NiFi can resume from where it left off")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(true)
        .build();
    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
        .name("Initial Start Position")
        .description("When the Processor first begins to tail data, this property specifies where the Processor should begin reading data. Once data has been ingested from the file, "
            + "the Processor will continue from the last point from which it has received data.")
        .allowableValues(START_BEGINNING_OF_TIME, START_CURRENT_FILE, START_CURRENT_TIME)
        .defaultValue(START_CURRENT_FILE.getValue())
        .required(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to this Relationship.")
        .build();

    private volatile TailFileState state = new TailFileState(null, null, null, 0L, 0L, null, ByteBuffer.allocate(65536));
    private volatile Long expectedRecoveryChecksum;
    private volatile boolean tailFileChanged = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILENAME);
        properties.add(ROLLING_FILENAME_PATTERN);
        properties.add(new PropertyDescriptor.Builder().fromPropertyDescriptor(STATE_FILE).defaultValue("./conf/state/" + getIdentifier()).build());
        properties.add(START_POSITION);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (FILENAME.equals(descriptor)) {
            state = new TailFileState(newValue, null, null, 0L, 0L, null, ByteBuffer.allocate(65536));
            tailFileChanged = true;
        }
    }

    @OnScheduled
    public void recoverState(final ProcessContext context) throws IOException {
        final String tailFilename = context.getProperty(FILENAME).getValue();
        final String stateFilename = context.getProperty(STATE_FILE).getValue();

        final File stateFile = new File(stateFilename);
        try (final FileInputStream fis = new FileInputStream(stateFile);
            final DataInputStream dis = new DataInputStream(fis)) {

            final int encodingVersion = dis.readInt();
            if (encodingVersion > 0) {
                throw new IOException("Unable to recover state because State File was encoded in a more recent version than Version 1");
            }

            if (encodingVersion == 0) {
                final String filename = dis.readUTF();
                long position = dis.readLong();
                final long timestamp = dis.readLong();
                final boolean checksumPresent = dis.readBoolean();

                FileChannel reader = null;
                File tailFile = null;

                if (checksumPresent && tailFilename.equals(filename)) {
                    expectedRecoveryChecksum = dis.readLong();

                    // We have an expected checksum and the currently configured filename is the same as the state file.
                    // We need to check if the existing file is the same as the one referred to in the state file based on
                    // the checksum.
                    final Checksum checksum = new CRC32();
                    final File existingTailFile = new File(filename);
                    if (existingTailFile.length() >= position) {
                        try (final InputStream tailFileIs = new FileInputStream(existingTailFile);
                            final CheckedInputStream in = new CheckedInputStream(tailFileIs, checksum)) {
                            StreamUtils.copy(in, new NullOutputStream(), state.getPosition());

                            final long checksumResult = in.getChecksum().getValue();
                            if (checksumResult == expectedRecoveryChecksum) {
                                // Checksums match. This means that we want to resume reading from where we left off.
                                // So we will populate the reader object so that it will be used in onTrigger. If the
                                // checksums do not match, then we will leave the reader object null, so that the next
                                // call to onTrigger will result in a new Reader being created and starting at the
                                // beginning of the file.
                                getLogger().debug("When recovering state, checksum of tailed file matches the stored checksum. Will resume where left off.");
                                tailFile = existingTailFile;
                                reader = FileChannel.open(tailFile.toPath(), StandardOpenOption.READ);
                                getLogger().debug("Created FileChannel {} for {} in recoverState", new Object[] {reader, tailFile});

                                reader.position(position);
                            } else {
                                // we don't seek the reader to the position, so our reader will start at beginning of file.
                                getLogger().debug("When recovering state, checksum of tailed file does not match the stored checksum. Will begin tailing current file from beginning.");
                            }
                        }
                    } else {
                        // fewer bytes than our position, so we know we weren't already reading from this file. Keep reader at a position of 0.
                        getLogger().debug("When recovering state, existing file to tail is only {} bytes but position flag is {}; "
                            + "this indicates that the file has rotated. Will begin tailing current file from beginning.", new Object[] {existingTailFile.length(), position});
                    }

                    state = new TailFileState(tailFilename, tailFile, reader, position, timestamp, checksum, ByteBuffer.allocate(65536));
                } else {
                    // If filename changed or there is no checksum present, then we have no expected checksum to use for recovery.
                    expectedRecoveryChecksum = null;

                    // tailing a new file since the state file was written out. We will reset state.
                    state = new TailFileState(tailFilename, null, null, 0L, 0L, null, ByteBuffer.allocate(65536));
                }

                getLogger().debug("Recovered state {}", new Object[] {state});
            } else {
                // encoding Version == -1... no data in file. Just move on.
            }
        } catch (final FileNotFoundException fnfe) {
        }
    }


    @OnStopped
    public void cleanup() {
        final TailFileState state = this.state;
        if (state == null) {
            return;
        }

        final FileChannel reader = state.getReader();
        if (reader == null) {
            return;
        }

        try {
            reader.close();
        } catch (final IOException ioe) {
            getLogger().warn("Failed to close file handle during cleanup");
        }

        getLogger().debug("Closed FileChannel {}", new Object[] {reader});

        this.state = new TailFileState(state.getFilename(), state.getFile(), null, state.getPosition(), state.getTimestamp(), state.getChecksum(), state.getBuffer());
    }



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // If user changes the file that is being tailed, we need to consume the already-rolled-over data according
        // to the Initial Start Position property
        boolean rolloverOccurred;
        if (tailFileChanged) {
            rolloverOccurred = false;
            final String recoverPosition = context.getProperty(START_POSITION).getValue();

            if (START_BEGINNING_OF_TIME.getValue().equals(recoverPosition)) {
                recoverRolledFiles(context, session, this.expectedRecoveryChecksum, state.getTimestamp(), state.getPosition());
            } else if (START_CURRENT_FILE.getValue().equals(recoverPosition)) {
                cleanup();
                state = new TailFileState(context.getProperty(FILENAME).getValue(), null, null, 0L, 0L, null, state.getBuffer());
            } else {
                final String filename = context.getProperty(FILENAME).getValue();
                final File file = new File(filename);

                try {
                    final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                    getLogger().debug("Created FileChannel {} for {}", new Object[] {fileChannel, file});

                    final Checksum checksum = new CRC32();
                    final long position = file.length();
                    final long timestamp = file.lastModified();

                    try (final InputStream fis = new FileInputStream(file);
                        final CheckedInputStream in = new CheckedInputStream(fis, checksum)) {
                        StreamUtils.copy(in, new NullOutputStream(), position);
                    }

                    fileChannel.position(position);
                    cleanup();
                    state = new TailFileState(filename, file, fileChannel, position, timestamp, checksum, state.getBuffer());
                } catch (final IOException ioe) {
                    getLogger().error("Attempted to position Reader at current position in file {} but failed to do so due to {}", new Object[] {file, ioe.toString()}, ioe);
                    context.yield();
                    return;
                }
            }

            tailFileChanged = false;
        } else {
            // Recover any data that may have rolled over since the last time that this processor ran.
            // If expectedRecoveryChecksum != null, that indicates that this is the first iteration since processor was started, so use whatever checksum value
            // was present when the state was last persisted. In this case, we must then null out the value so that the next iteration won't keep using the "recovered"
            // value. If the value is null, then we know that either the processor has already recovered that data, or there was no state persisted. In either case,
            // use whatever checksum value is currently in the state.
            Long expectedChecksumValue = expectedRecoveryChecksum;
            if (expectedChecksumValue == null) {
                expectedChecksumValue = state.getChecksum() == null ? null : state.getChecksum().getValue();
            }

            rolloverOccurred = recoverRolledFiles(context, session, expectedChecksumValue, state.getTimestamp(), state.getPosition());
            expectedRecoveryChecksum = null;
        }

        // initialize local variables from state object; this is done so that we can easily change the values throughout
        // the onTrigger method and then create a new state object after we finish processing the files.
        TailFileState state = this.state;
        File file = state.getFile();
        FileChannel reader = state.getReader();
        Checksum checksum = state.getChecksum();
        if (checksum == null) {
            checksum = new CRC32();
        }
        long position = state.getPosition();
        long timestamp = state.getTimestamp();

        // Create a reader if necessary.
        if (file == null || reader == null) {
            file = new File(context.getProperty(FILENAME).getValue());
            reader = createReader(file, position);
            if (reader == null) {
                context.yield();
                return;
            }
        }

        final long startNanos = System.nanoTime();

        // Check if file has rotated
        if (rolloverOccurred) {
            // Since file has rotated, we close the reader, create a new one, and then reset our state.
            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", new Object[] {reader, reader});
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close reader for {} due to {}", new Object[] {file, ioe});
            }

            reader = createReader(file, 0L);
            position = 0L;
            checksum.reset();
        }

        if (file.length() == position) {
            // no data to consume so rather than continually running, yield to allow other processors to use the thread.
            // In this case, the state should not have changed, and we will have created no FlowFiles, so we don't have to
            // persist the state or commit the session; instead, just return here.
            getLogger().debug("No data to consume; created no FlowFiles");
            state = this.state = new TailFileState(context.getProperty(FILENAME).getValue(), file, reader, position, timestamp, checksum, state.getBuffer());
            persistState(state, context);
            context.yield();
            return;
        }

        // If there is data to consume, read as much as we can.
        final TailFileState currentState = state;
        final Checksum chksum = checksum;
        // data has been written to file. Stream it to a new FlowFile.
        FlowFile flowFile = session.create();

        final FileChannel fileReader = reader;
        final LongHolder positionHolder = new LongHolder(position);
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream rawOut) throws IOException {
                try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                    positionHolder.set(readLines(fileReader, currentState.getBuffer(), out, chksum));
                }
            }
        });

        // If there ended up being no data, just remove the FlowFile
        if (flowFile.getSize() == 0) {
            session.remove(flowFile);
            getLogger().debug("No data to consume; removed created FlowFile");
        } else {
            // determine filename for FlowFile by using <base filename of log file>.<initial offset>-<final offset>.<extension>
            final String tailFilename = file.getName();
            final String baseName = StringUtils.substringBeforeLast(tailFilename, ".");
            final String flowFileName;
            if (baseName.length() < tailFilename.length()) {
                flowFileName = baseName + "." + position + "-" + positionHolder.get() + "." + StringUtils.substringAfterLast(tailFilename, ".");
            } else {
                flowFileName = baseName + "." + position + "-" + positionHolder.get();
            }

            final Map<String, String> attributes = new HashMap<>(2);
            attributes.put(CoreAttributes.FILENAME.key(), flowFileName);
            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().receive(flowFile, file.toURI().toString(), "FlowFile contains bytes " + position + " through " + positionHolder.get() + " of source file",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            session.transfer(flowFile, REL_SUCCESS);
            position = positionHolder.get();

            // Set timestamp to the latest of when the file was modified and the current timestamp stored in the state.
            // We do this because when we read a file that has been rolled over, we set the state to 1 millisecond later than the last mod date
            // in order to avoid ingesting that file again. If we then read from this file during the same second (or millisecond, depending on the
            // operating system file last mod precision), then we could set the timestamp to a smaller value, which could result in reading in the
            // rotated file a second time.
            timestamp = Math.max(state.getTimestamp(), file.lastModified());
            getLogger().debug("Created {} and routed to success", new Object[] {flowFile});
        }

        // Create a new state object to represent our current position, timestamp, etc.
        final TailFileState updatedState = new TailFileState(context.getProperty(FILENAME).getValue(), file, reader, position, timestamp, checksum, state.getBuffer());
        this.state = updatedState;

        // We must commit session before persisting state in order to avoid data loss on restart
        session.commit();
        persistState(updatedState, context);
    }


    /**
     * Read new lines from the given FileChannel, copying it to the given Output Stream. The Checksum is used in order to later determine whether or not
     * data has been consumed.
     *
     * @param reader The FileChannel to read data from
     * @param buffer the buffer to use for copying data
     * @param out the OutputStream to copy the data to
     * @param checksum the Checksum object to use in order to calculate checksum for recovery purposes
     *
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(final FileChannel reader, final ByteBuffer buffer, final OutputStream out, final Checksum checksum) throws IOException {
        getLogger().debug("Reading lines starting at position {}", new Object[] {reader.position()});

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            long pos = reader.position();
            long rePos = pos; // position to re-read

            int num;
            int linesRead = 0;
            boolean seenCR = false;
            buffer.clear();

            while (((num = reader.read(buffer)) != -1)) {
                buffer.flip();

                for (int i = 0; i < num; i++) {
                    byte ch = buffer.get(i);

                    switch (ch) {
                        case '\n':
                            baos.write(ch);
                            seenCR = false;
                            baos.writeTo(out);
                            checksum.update(baos.getUnderlyingBuffer(), 0, baos.size());
                            if (getLogger().isTraceEnabled()) {
                                getLogger().trace("Checksum updated to {}", new Object[] {checksum.getValue()});
                            }

                            baos.reset();
                            rePos = pos + i + 1;
                            linesRead++;
                            break;
                        case '\r':
                            baos.write(ch);
                            seenCR = true;
                            break;
                        default:
                            if (seenCR) {
                                seenCR = false;
                                baos.writeTo(out);
                                checksum.update(baos.getUnderlyingBuffer(), 0, baos.size());
                                if (getLogger().isTraceEnabled()) {
                                    getLogger().trace("Checksum updated to {}", new Object[] {checksum.getValue()});
                                }

                                linesRead++;
                                baos.reset();
                                baos.write(ch);
                                rePos = pos + i;
                            } else {
                                baos.write(ch);
                            }
                    }
                }

                pos = reader.position();
            }

            if (rePos < reader.position()) {
                getLogger().debug("Read {} lines; repositioning reader from {} to {}", new Object[] {linesRead, pos, rePos});
                reader.position(rePos); // Ensure we can re-read if necessary
            }

            return rePos;
        }
    }


    /**
     * Returns a list of all Files that match the following criteria:
     *
     * <ul>
     * <li>Filename matches the Rolling Filename Pattern</li>
     * <li>Filename does not match the actual file being tailed</li>
     * <li>The Last Modified Time on the file is equal to or later than the given minimum timestamp</li>
     * </ul>
     *
     * <p>
     * The List that is returned will be ordered by file timestamp, providing the oldest file first.
     * </p>
     *
     * @param context the ProcessContext to use in order to determine Processor configuration
     * @param minTimestamp any file with a Last Modified Time before this timestamp will not be returned
     * @return a list of all Files that have rolled over
     * @throws IOException if unable to perform the listing of files
     */
    private List<File> getRolledOffFiles(final ProcessContext context, final long minTimestamp) throws IOException {
        final String tailFilename = context.getProperty(FILENAME).getValue();
        final File tailFile = new File(tailFilename);
        File directory = tailFile.getParentFile();
        if (directory == null) {
            directory = new File(".");
        }

        final String rollingPattern = context.getProperty(ROLLING_FILENAME_PATTERN).getValue();
        if (rollingPattern == null) {
            return Collections.emptyList();
        }

        final List<File> rolledOffFiles = new ArrayList<>();
        final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory.toPath(), rollingPattern);
        for (final Path path : dirStream) {
            final File file = path.toFile();
            final long lastMod = file.lastModified();

            if (file.lastModified() < minTimestamp) {
                getLogger().debug("Found rolled off file {} but its last modified timestamp is before the cutoff (Last Mod = {}, Cutoff = {}) so will not consume it",
                    new Object[] {file, lastMod, minTimestamp});

                continue;
            } else if (file.equals(tailFile)) {
                continue;
            }

            rolledOffFiles.add(file);
        }

        // Sort files based on last modified timestamp. If same timestamp, use filename as a secondary sort, as often
        // files that are rolled over are given a naming scheme that is lexicographically sort in the same order as the
        // timestamp, such as yyyy-MM-dd-HH-mm-ss
        Collections.sort(rolledOffFiles, new Comparator<File>() {
            @Override
            public int compare(final File o1, final File o2) {
                final int lastModifiedComp = Long.compare(o1.lastModified(), o2.lastModified());
                if (lastModifiedComp != 0) {
                    return lastModifiedComp;
                }

                return o1.getName().compareTo(o2.getName());
            }
        });

        return rolledOffFiles;
    }



    private void persistState(final TailFileState state, final ProcessContext context) {
        final String stateFilename = context.getProperty(STATE_FILE).getValue();
        try {
            persistState(state, stateFilename);
        } catch (final IOException e) {
            getLogger().warn("Failed to update state file {} due to {}; some data may be duplicated on restart of NiFi", new Object[] {stateFilename, e});
        }
    }

    private void persistState(final TailFileState state, final String stateFilename) throws IOException {
        getLogger().debug("Persisting state {} to {}", new Object[] {state, stateFilename});

        final File stateFile = new File(stateFilename);
        File directory = stateFile.getParentFile();
        if (directory != null && !directory.exists() && !directory.mkdirs()) {
            getLogger().warn("Failed to persist state to {} because the parent directory does not exist and could not be created. This may result in data being duplicated upon restart of NiFi");
            return;
        }
        try (final FileOutputStream fos = new FileOutputStream(stateFile);
            final DataOutputStream dos = new DataOutputStream(fos)) {

            dos.writeInt(0); // version
            dos.writeUTF(state.getFilename());
            dos.writeLong(state.getPosition());
            dos.writeLong(state.getTimestamp());
            if (state.getChecksum() == null) {
                dos.writeBoolean(false);
            } else {
                dos.writeBoolean(true);
                dos.writeLong(state.getChecksum().getValue());
            }
        }
    }

    private FileChannel createReader(final File file, final long position) {
        final FileChannel reader;

        try {
            reader = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (final IOException ioe) {
            getLogger().warn("Unable to open file {}; will attempt to access file again after the configured Yield Duration has elapsed: {}", new Object[] {file, ioe});
            return null;
        }

        getLogger().debug("Created FileChannel {} for {}", new Object[] {reader, file});

        try {
            reader.position(position);
        } catch (final IOException ioe) {
            getLogger().error("Failed to read from {} due to {}", new Object[] {file, ioe});

            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", new Object[] {reader});
            } catch (final IOException ioe2) {
            }

            return null;
        }

        return reader;
    }

    // for testing purposes
    TailFileState getState() {
        return state;
    }


    /**
     * Finds any files that have rolled over and have not yet been ingested by this Processor. Each of these files that is found will be
     * ingested as its own FlowFile. If a file is found that has been partially ingested, the rest of the file will be ingested as a
     * single FlowFile but the data that already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor configuration.
     * @param session the ProcessSession to use in order to interact with FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the oldest file from offset 0 through &lt;position&gt;.
     * @param timestamp the latest Last Modified Timestamp that has been consumed. Any data that was written before this data will not be ingested.
     * @param position the byte offset in the file being tailed, where tailing last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over, <code>false</code> otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final Long expectedChecksum, final long timestamp, final long position) {
        try {
            // Find all files that match our rollover pattern, if any, and order them based on their timestamp and filename.
            // Ignore any file that has a timestamp earlier than the state that we have persisted. If we were reading from
            // a file when we stopped running, then that file that we were reading from should be the first file in this list,
            // assuming that the file still exists on the file system.
            final List<File> rolledOffFiles = getRolledOffFiles(context, timestamp);
            return recoverRolledFiles(context, session, rolledOffFiles, expectedChecksum, timestamp, position);
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over due to {}", new Object[] {e});
            return false;
        }
    }

    /**
     * Finds any files that have rolled over and have not yet been ingested by this Processor. Each of these files that is found will be
     * ingested as its own FlowFile. If a file is found that has been partially ingested, the rest of the file will be ingested as a
     * single FlowFile but the data that already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor configuration.
     * @param session the ProcessSession to use in order to interact with FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the oldest file from offset 0 through &lt;position&gt;.
     * @param timestamp the latest Last Modfiied Timestamp that has been consumed. Any data that was written before this data will not be ingested.
     * @param position the byte offset in the file being tailed, where tailing last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over, false otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final List<File> rolledOffFiles, final Long expectedChecksum,
        final long timestamp, final long position) {
        try {
            getLogger().debug("Recovering Rolled Off Files; total number of files rolled off = {}", new Object[] {rolledOffFiles.size()});

            // For first file that we find, it may or may not be the file that we were last reading from.
            // As a result, we have to read up to the position we stored, while calculating the checksum. If the checksums match,
            // then we know we've already processed this file. If the checksums do not match, then we have not
            // processed this file and we need to seek back to position 0 and ingest the entire file.
            // For all other files that have been rolled over, we need to just ingest the entire file.
            boolean rolloverOccurred = !rolledOffFiles.isEmpty();
            if (rolloverOccurred && expectedChecksum != null && rolledOffFiles.get(0).length() >= position) {
                final File firstFile = rolledOffFiles.get(0);

                final long startNanos = System.nanoTime();
                if (position > 0) {
                    try (final InputStream fis = new FileInputStream(firstFile);
                        final CheckedInputStream in = new CheckedInputStream(fis, new CRC32())) {
                        StreamUtils.copy(in, new NullOutputStream(), position);

                        final long checksumResult = in.getChecksum().getValue();
                        if (checksumResult == expectedChecksum) {
                            getLogger().debug("Checksum for {} matched expected checksum. Will skip first {} bytes", new Object[] {firstFile, position});

                            // This is the same file that we were reading when we shutdown. Start reading from this point on.
                            rolledOffFiles.remove(0);
                            FlowFile flowFile = session.create();
                            flowFile = session.importFrom(in, flowFile);
                            if (flowFile.getSize() == 0L) {
                                session.remove(flowFile);
                                // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
                                cleanup();
                                state = new TailFileState(context.getProperty(FILENAME).getValue(), null, null, 0L, firstFile.lastModified() + 1L, null, state.getBuffer());
                            } else {
                                flowFile = session.putAttribute(flowFile, "filename", firstFile.getName());

                                session.getProvenanceReporter().receive(flowFile, firstFile.toURI().toString(), "FlowFile contains bytes 0 through " + position + " of source file",
                                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                                session.transfer(flowFile, REL_SUCCESS);
                                getLogger().debug("Created {} from rolled over file {} and routed to success", new Object[] {flowFile, firstFile});

                                // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
                                cleanup();
                                state = new TailFileState(context.getProperty(FILENAME).getValue(), null, null, 0L, firstFile.lastModified() + 1L, null, state.getBuffer());

                                // must ensure that we do session.commit() before persisting state in order to avoid data loss.
                                session.commit();
                                persistState(state, context.getProperty(STATE_FILE).getValue());
                            }
                        } else {
                            getLogger().debug("Checksum for {} did not match expected checksum. Checksum for file was {} but expected {}. Will consume entire file",
                                new Object[] {firstFile, checksumResult, expectedChecksum});
                        }
                    }
                }
            }

            // For each file that we found that matches our Rollover Pattern, and has a last modified date later than the timestamp
            // that we recovered from the state file, we need to consume the entire file. The only exception to this is the file that
            // we were reading when we last stopped, as it may already have been partially consumed. That is taken care of in the
            // above block of code.
            for (final File file : rolledOffFiles) {
                state = consumeFileFully(file, context, session, state);
            }

            return rolloverOccurred;
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over due to {}", new Object[] {e});
            return false;
        }
    }


    /**
     * Creates a new FlowFile that contains the entire contents of the given file and transfers that FlowFile to success. This method
     * will commit the given session and emit an appropriate Provenance Event.
     *
     * @param file the file to ingest
     * @param context the ProcessContext
     * @param session the ProcessSession
     * @param state the current state
     *
     * @return the new, updated state that reflects that the given file has been ingested.
     */
    private TailFileState consumeFileFully(final File file, final ProcessContext context, final ProcessSession session, TailFileState state) {
        FlowFile flowFile = session.create();
        flowFile = session.importFrom(file.toPath(), true, flowFile);
        if (flowFile.getSize() == 0L) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAttribute(flowFile, "filename", file.getName());
            session.getProvenanceReporter().receive(flowFile, file.toURI().toString());
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Created {} from {} and routed to success", new Object[] {flowFile, file});

            // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
            cleanup();
            state = new TailFileState(context.getProperty(FILENAME).getValue(), null, null, 0L, file.lastModified() + 1L, null, state.getBuffer());

            // must ensure that we do session.commit() before persisting state in order to avoid data loss.
            session.commit();
            persistState(state, context);
        }

        return state;
    }

    /**
     * A simple Java class to hold information about our state so that we can maintain this state across multiple invocations of the Processor
     */
    static class TailFileState {
        private final String filename; // hold onto filename and not just File because we want to match that against the user-defined filename to recover from
        private final File file;
        private final FileChannel reader;
        private final long position;
        private final long timestamp;
        private final Checksum checksum;
        private final ByteBuffer buffer;

        public TailFileState(final String filename, final File file, final FileChannel reader, final long position, final long timestamp, final Checksum checksum, final ByteBuffer buffer) {
            this.filename = filename;
            this.file = file;
            this.reader = reader;
            this.position = position;
            this.timestamp = timestamp; // many operating systems will use only second-level precision for last-modified times so cut off milliseconds
            this.checksum = checksum;
            this.buffer = buffer;
        }

        public String getFilename() {
            return filename;
        }

        public File getFile() {
            return file;
        }

        public FileChannel getReader() {
            return reader;
        }

        public long getPosition() {
            return position;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Checksum getChecksum() {
            return checksum;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        public String toString() {
            return "TailFileState[filename=" + filename + ", position=" + position + ", timestamp=" + timestamp + ", checksum=" + (checksum == null ? "null" : checksum.getValue()) + "]";
        }
    }
}
