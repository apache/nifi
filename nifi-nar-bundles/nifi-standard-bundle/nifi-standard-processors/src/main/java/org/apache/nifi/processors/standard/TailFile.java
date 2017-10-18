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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

// note: it is important that this Processor is not marked as @SupportsBatching because the session commits must complete before persisting state locally; otherwise, data loss may occur
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"tail", "file", "log", "text", "source", "restricted"})
@CapabilityDescription("\"Tails\" a file, or a list of files, ingesting data from the file as it is written to the file. The file is expected to be textual. Data is ingested only when a "
        + "new line is encountered (carriage return or new-line character or combination). If the file to tail is periodically \"rolled over\", as is generally the case "
        + "with log files, an optional Rolling Filename Pattern can be used to retrieve data from files that have rolled over, even if the rollover occurred while NiFi "
        + "was not running (provided that the data still exists upon restart of NiFi). It is generally advisable to set the Run Schedule to a few seconds, rather than running "
        + "with the default value of 0 secs, as this Processor will consume a lot of resources if scheduled very aggressively. At this time, this Processor does not support "
        + "ingesting files that have been compressed when 'rolled over'.")
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "Stores state about where in the Tailed File it left off so that on restart it does not have to duplicate data. "
        + "State is stored either local or clustered depend on the <File Location> property.")
@WritesAttributes({
    @WritesAttribute(attribute = "tailfile.original.path", description = "Path of the original file the flow file comes from.")
    })
@Restricted("Provides operator the ability to read from any file that NiFi has access to.")
public class TailFile extends AbstractProcessor {

    static final String MAP_PREFIX = "file.";

    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local",
            "State is stored locally. Each node in a cluster will tail a different file.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote",
            "State is located on a remote resource. This Processor will store state across the cluster so that "
            + "it can be run on Primary Node Only and a new Primary Node can pick up where the last one left off.");

    static final AllowableValue MODE_SINGLEFILE = new AllowableValue("Single file", "Single file",
            "In this mode, only the one file indicated in the 'Files to tail' property will be watched by the processor."
            + " In this mode, the file may not exist when starting the processor.");
    static final AllowableValue MODE_MULTIFILE = new AllowableValue("Multiple files", "Multiple files",
            "In this mode, the 'Files to tail' property accepts a regular expression and the processor will look"
            + " for files in 'Base directory' to list the files to tail by the processor.");

    static final AllowableValue FIXED_NAME = new AllowableValue("Fixed name", "Fixed name", "With this rolling strategy, the files "
            + "where the log messages are appended have always the same name.");
    static final AllowableValue CHANGING_NAME = new AllowableValue("Changing name", "Changing name", "With this rolling strategy, "
            + "the files where the log messages are appended have not a fixed name (for example: filename contaning the current day.");

    static final AllowableValue START_BEGINNING_OF_TIME = new AllowableValue("Beginning of Time", "Beginning of Time",
            "Start with the oldest data that matches the Rolling Filename Pattern and then begin reading from the File to Tail");
    static final AllowableValue START_CURRENT_FILE = new AllowableValue("Beginning of File", "Beginning of File",
            "Start with the beginning of the File to Tail. Do not ingest any data that has already been rolled over");
    static final AllowableValue START_CURRENT_TIME = new AllowableValue("Current Time", "Current Time",
            "Start with the data at the end of the File to Tail. Do not ingest any data thas has already been rolled over or any "
            + "data in the File to Tail that has already been written.");

    static final PropertyDescriptor BASE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("tail-base-directory")
            .displayName("Base directory")
            .description("Base directory used to look for files to tail. This property is required when using Multifile mode.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("tail-mode")
            .displayName("Tailing mode")
            .description("Mode to use: single file will tail only one file, multiple file will look for a list of file. In Multiple mode"
                    + " the Base directory is required.")
            .expressionLanguageSupported(false)
            .required(true)
            .allowableValues(MODE_SINGLEFILE, MODE_MULTIFILE)
            .defaultValue(MODE_SINGLEFILE.getValue())
            .build();

    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .displayName("File(s) to Tail")
            .name("File to Tail")
            .description("Path of the file to tail in case of single file mode. If using multifile mode, regular expression to find files "
                    + "to tail in the base directory. In case recursivity is set to true, the regular expression will be used to match the "
                    + "path starting from the base directory (see additional details for examples).")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .required(true)
            .build();

    static final PropertyDescriptor ROLLING_FILENAME_PATTERN = new PropertyDescriptor.Builder()
            .name("Rolling Filename Pattern")
            .description("If the file to tail \"rolls over\" as would be the case with log files, this filename pattern will be used to "
                    + "identify files that have rolled over so that if NiFi is restarted, and the file has rolled over, it will be able to pick up where it left off. "
                    + "This pattern supports wildcard characters * and ?, it also supports the notation ${filename} to specify a pattern based on the name of the file "
                    + "(without extension), and will assume that the files that have rolled over live in the same directory as the file being tailed. "
                    + "The same glob pattern will be used for all files.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(false)
            .build();

    static final PropertyDescriptor STATE_LOCATION = new PropertyDescriptor.Builder()
            .displayName("State Location")
            .name("File Location") //retained name of property for backward compatibility of configs
            .description("Specifies where the state is located either local or cluster so that state can be stored "
                    + "appropriately in order to ensure that all data is consumed without duplicating data upon restart of NiFi")
            .required(true)
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .build();

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
            .name("Initial Start Position")
            .description("When the Processor first begins to tail data, this property specifies where the Processor should begin reading data. Once data has been ingested from a file, "
                    + "the Processor will continue from the last point from which it has received data.")
            .allowableValues(START_BEGINNING_OF_TIME, START_CURRENT_FILE, START_CURRENT_TIME)
            .defaultValue(START_CURRENT_FILE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor RECURSIVE = new PropertyDescriptor.Builder()
            .name("tailfile-recursive-lookup")
            .displayName("Recursive lookup")
            .description("When using Multiple files mode, this property defines if files must be listed recursively or not"
                    + " in the base directory.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor ROLLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("tailfile-rolling-strategy")
            .displayName("Rolling Strategy")
            .description("Specifies if the files to tail have a fixed name or not.")
            .required(true)
            .allowableValues(FIXED_NAME, CHANGING_NAME)
            .defaultValue(FIXED_NAME.getValue())
            .build();

    static final PropertyDescriptor LOOKUP_FREQUENCY = new PropertyDescriptor.Builder()
            .name("tailfile-lookup-frequency")
            .displayName("Lookup frequency")
            .description("Only used in Multiple files mode and Changing name rolling strategy. It specifies the minimum "
                    + "duration the processor will wait before listing again the files to tail.")
            .required(false)
            .defaultValue("10 minutes")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_AGE = new PropertyDescriptor.Builder()
            .name("tailfile-maximum-age")
            .displayName("Maximum age")
            .description("Only used in Multiple files mode and Changing name rolling strategy. It specifies the necessary "
                    + "minimum duration to consider that no new messages will be appended in a file regarding its last "
                    + "modification date. This should not be set too low to avoid duplication of data in case new messages "
                    + "are appended at a lower frequency.")
            .required(false)
            .defaultValue("24 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this Relationship.")
            .build();

    private volatile Map<String, TailFileObject> states = new HashMap<String, TailFileObject>();
    private volatile AtomicLong lastLookup = new AtomicLong(0L);
    private volatile AtomicBoolean isMultiChanging = new AtomicBoolean(false);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(FILENAME);
        properties.add(ROLLING_FILENAME_PATTERN);
        properties.add(BASE_DIRECTORY);
        properties.add(START_POSITION);
        properties.add(STATE_LOCATION);
        properties.add(RECURSIVE);
        properties.add(ROLLING_STRATEGY);
        properties.add(LOOKUP_FREQUENCY);
        properties.add(MAXIMUM_AGE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && FILENAME.equals(descriptor)) {
            states = new HashMap<String, TailFileObject>();
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        if(context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue())) {
            String path = context.getProperty(BASE_DIRECTORY).evaluateAttributeExpressions().getValue();
            if(path == null) {
                results.add(new ValidationResult.Builder().subject(BASE_DIRECTORY.getName()).valid(false)
                        .explanation("Base directory property cannot be empty in Multifile mode.").build());
            } else if (!new File(path).isDirectory()) {
                results.add(new ValidationResult.Builder().subject(BASE_DIRECTORY.getName()).valid(false)
                            .explanation(path + " is not a directory.").build());
            }

            if(context.getProperty(ROLLING_STRATEGY).getValue().equals(CHANGING_NAME.getValue())) {
                String freq = context.getProperty(LOOKUP_FREQUENCY).getValue();
                if(freq == null) {
                    results.add(new ValidationResult.Builder().subject(LOOKUP_FREQUENCY.getName()).valid(false)
                            .explanation("In Multiple files mode and Changing name rolling strategy, lookup frequency "
                                    + "property must be specified.").build());
                }
                String maxAge = context.getProperty(MAXIMUM_AGE).getValue();
                if(maxAge == null) {
                    results.add(new ValidationResult.Builder().subject(MAXIMUM_AGE.getName()).valid(false)
                            .explanation("In Multiple files mode and Changing name rolling strategy, maximum age "
                                    + "property must be specified.").build());
                }
            } else {
                long max = context.getProperty(MAXIMUM_AGE).getValue() == null ? Long.MAX_VALUE : context.getProperty(MAXIMUM_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
                List<String> filesToTail = getFilesToTail(context.getProperty(BASE_DIRECTORY).evaluateAttributeExpressions().getValue(),
                        context.getProperty(FILENAME).evaluateAttributeExpressions().getValue(),
                        context.getProperty(RECURSIVE).asBoolean(),
                        max);

                if(filesToTail.isEmpty()) {
                    results.add(new ValidationResult.Builder().subject(FILENAME.getName()).valid(false)
                                .explanation("There is no file to tail. Files must exist when starting this processor.").build());
                }
            }
        }

        return results;
    }

    @OnScheduled
    public void recoverState(final ProcessContext context) throws IOException {
        // set isMultiChanging
        isMultiChanging.set(context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue()));

        // set last lookup to now
        lastLookup.set(new Date().getTime());

        // maxAge
        long maxAge = context.getProperty(MAXIMUM_AGE).getValue() == null ? Long.MAX_VALUE : context.getProperty(MAXIMUM_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        // get list of files to tail
        List<String> filesToTail = new ArrayList<String>();

        if(context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue())) {
            filesToTail.addAll(getFilesToTail(context.getProperty(BASE_DIRECTORY).evaluateAttributeExpressions().getValue(),
                    context.getProperty(FILENAME).evaluateAttributeExpressions().getValue(),
                    context.getProperty(RECURSIVE).asBoolean(),
                    maxAge));
        } else {
            filesToTail.add(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue());
        }


        final Scope scope = getStateScope(context);
        final StateMap stateMap = context.getStateManager().getState(scope);

        if (stateMap.getVersion() == -1L) {
            //state has been cleared or never stored so recover as 'empty state'
            initStates(filesToTail, Collections.emptyMap(), true);
            recoverState(context, filesToTail, Collections.emptyMap());
            return;
        }

        Map<String, String> statesMap = stateMap.toMap();

        if (statesMap.containsKey(TailFileState.StateKeys.FILENAME)
                && !statesMap.keySet().stream().anyMatch(key -> key.startsWith(MAP_PREFIX))) {
            // If statesMap contains "filename" key without "file.0." prefix,
            // and there's no key with "file." prefix, then
            // it indicates that the statesMap is created with earlier version of NiFi.
            // In this case, we need to migrate the state by adding prefix indexed with 0.
            final Map<String, String> migratedStatesMap = new HashMap<>(statesMap.size());
            for (String key : statesMap.keySet()) {
                migratedStatesMap.put(MAP_PREFIX + "0." + key, statesMap.get(key));
            }

            // LENGTH is added from NiFi 1.1.0. Set the value with using the last position so that we can use existing state
            // to avoid sending duplicated log data after updating NiFi.
            migratedStatesMap.put(MAP_PREFIX + "0." + TailFileState.StateKeys.LENGTH, statesMap.get(TailFileState.StateKeys.POSITION));
            statesMap = Collections.unmodifiableMap(migratedStatesMap);

            getLogger().info("statesMap has been migrated. {}", new Object[]{migratedStatesMap});
        }

        initStates(filesToTail, statesMap, false);
        recoverState(context, filesToTail, statesMap);
    }

    private void initStates(List<String> filesToTail, Map<String, String> statesMap, boolean isCleared) {
        int i = 0;

        if(isCleared) {
            states.clear();
        } else {
            // we have to deal with the case where NiFi has been restarted. In this
            // case 'states' object is empty but the statesMap is not. So we have to
            // put back the files we already know about in 'states' object before
            // doing the recovery
            if(states.isEmpty() && !statesMap.isEmpty()) {
                for(String key : statesMap.keySet()) {
                    if(key.endsWith(TailFileState.StateKeys.FILENAME)) {
                        int index = Integer.valueOf(key.split("\\.")[1]);
                        states.put(statesMap.get(key), new TailFileObject(index, statesMap));
                    }
                }
            }

            // first, we remove the files that are no longer present
            List<String> toBeRemoved = new ArrayList<String>();
            for(String file : states.keySet()) {
                if(!filesToTail.contains(file)) {
                    toBeRemoved.add(file);
                    cleanReader(states.get(file));
                }
            }
            states.keySet().removeAll(toBeRemoved);

            // then we need to get the highest ID used so far to be sure
            // we don't mix different files in case we add new files to tail
            for(String file : states.keySet()) {
                if(i <= states.get(file).getFilenameIndex()) {
                    i = states.get(file).getFilenameIndex() + 1;
                }
            }

        }

        for (String file : filesToTail) {
            if(isCleared || !states.containsKey(file)) {
                states.put(file, new TailFileObject(i));
                i++;
            }
        }

    }

    private void recoverState(final ProcessContext context, final List<String> filesToTail, final Map<String, String> map) throws IOException {
        for (String file : filesToTail) {
            recoverState(context, map, file);
        }
    }

    /**
     * Method to list the files to tail according to the given base directory
     * and using the user-provided regular expression
     * @param baseDir base directory to recursively look into
     * @param fileRegex expression regular used to match files to tail
     * @param isRecursive true if looking for file recursively, false otherwise
     * @return List of files to tail
     */
    private List<String> getFilesToTail(final String baseDir, String fileRegex, boolean isRecursive, long maxAge) {
        Collection<File> files = FileUtils.listFiles(new File(baseDir), null, isRecursive);
        List<String> result = new ArrayList<String>();

        String baseDirNoTrailingSeparator = baseDir.endsWith(File.separator) ? baseDir.substring(0, baseDir.length() -1) : baseDir;
        final String fullRegex;
        if (File.separator.equals("/")) {
            // handle unix-style paths
            fullRegex = baseDirNoTrailingSeparator + File.separator + fileRegex;
        } else {
            // handle windows-style paths, need to quote backslash characters
            fullRegex = baseDirNoTrailingSeparator + Pattern.quote(File.separator) + fileRegex;
        }
        Pattern p = Pattern.compile(fullRegex);

        for(File file : files) {
            String path = file.getPath();
            if(p.matcher(path).matches()) {
                if(isMultiChanging.get()) {
                    if((new Date().getTime() - file.lastModified()) < maxAge) {
                        result.add(path);
                    }
                } else {
                    result.add(path);
                }
            }
        }

        return result;
    }

    /**
     * Updates member variables to reflect the "expected recovery checksum" and
     * seek to the appropriate location in the tailed file, updating our
     * checksum, so that we are ready to proceed with the
     * {@link #onTrigger(ProcessContext, ProcessSession)} call.
     *
     * @param context the ProcessContext
     * @param stateValues the values that were recovered from state that was
     * previously stored. This Map should be populated with the keys defined in
     * {@link TailFileState.StateKeys}.
     * @param filePath the file of the file for which state must be recovered
     * @throws IOException if unable to seek to the appropriate location in the
     * tailed file.
     */
    private void recoverState(final ProcessContext context, final Map<String, String> stateValues, final String filePath) throws IOException {

        final String prefix = MAP_PREFIX + states.get(filePath).getFilenameIndex() + '.';

        if (!stateValues.containsKey(prefix + TailFileState.StateKeys.FILENAME)) {
            resetState(filePath);
            return;
        }
        if (!stateValues.containsKey(prefix + TailFileState.StateKeys.POSITION)) {
            resetState(filePath);
            return;
        }
        if (!stateValues.containsKey(prefix + TailFileState.StateKeys.TIMESTAMP)) {
            resetState(filePath);
            return;
        }
        if (!stateValues.containsKey(prefix + TailFileState.StateKeys.LENGTH)) {
            resetState(filePath);
            return;
        }

        final String checksumValue = stateValues.get(prefix + TailFileState.StateKeys.CHECKSUM);
        final boolean checksumPresent = (checksumValue != null);
        final String storedStateFilename = stateValues.get(prefix + TailFileState.StateKeys.FILENAME);
        final long position = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.POSITION));
        final long timestamp = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.TIMESTAMP));
        final long length = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.LENGTH));

        FileChannel reader = null;
        File tailFile = null;

        if (checksumPresent && filePath.equals(storedStateFilename)) {
            states.get(filePath).setExpectedRecoveryChecksum(Long.parseLong(checksumValue));

            // We have an expected checksum and the currently configured filename is the same as the state file.
            // We need to check if the existing file is the same as the one referred to in the state file based on
            // the checksum.
            final Checksum checksum = new CRC32();
            final File existingTailFile = new File(storedStateFilename);
            if (existingTailFile.length() >= position) {
                try (final InputStream tailFileIs = new FileInputStream(existingTailFile);
                        final CheckedInputStream in = new CheckedInputStream(tailFileIs, checksum)) {
                    StreamUtils.copy(in, new NullOutputStream(), states.get(filePath).getState().getPosition());

                    final long checksumResult = in.getChecksum().getValue();
                    if (checksumResult == states.get(filePath).getExpectedRecoveryChecksum()) {
                        // Checksums match. This means that we want to resume reading from where we left off.
                        // So we will populate the reader object so that it will be used in onTrigger. If the
                        // checksums do not match, then we will leave the reader object null, so that the next
                        // call to onTrigger will result in a new Reader being created and starting at the
                        // beginning of the file.
                        getLogger().debug("When recovering state, checksum of tailed file matches the stored checksum. Will resume where left off.");
                        tailFile = existingTailFile;
                        reader = FileChannel.open(tailFile.toPath(), StandardOpenOption.READ);
                        getLogger().debug("Created FileChannel {} for {} in recoverState", new Object[]{reader, tailFile});

                        reader.position(position);
                    } else {
                        // we don't seek the reader to the position, so our reader will start at beginning of file.
                        getLogger().debug("When recovering state, checksum of tailed file does not match the stored checksum. Will begin tailing current file from beginning.");
                    }
                }
            } else {
                // fewer bytes than our position, so we know we weren't already reading from this file. Keep reader at a position of 0.
                getLogger().debug("When recovering state, existing file to tail is only {} bytes but position flag is {}; "
                        + "this indicates that the file has rotated. Will begin tailing current file from beginning.", new Object[]{existingTailFile.length(), position});
            }

            states.get(filePath).setState(new TailFileState(filePath, tailFile, reader, position, timestamp, length, checksum, ByteBuffer.allocate(65536)));
        } else {
            resetState(filePath);
        }

        getLogger().debug("Recovered state {}", new Object[]{states.get(filePath).getState()});
    }

    private void resetState(final String filePath) {
        states.get(filePath).setExpectedRecoveryChecksum(null);
        states.get(filePath).setState(new TailFileState(filePath, null, null, 0L, 0L, 0L, null, ByteBuffer.allocate(65536)));
    }

    @OnStopped
    public void cleanup() {
        for (TailFileObject tfo : states.values()) {
            cleanReader(tfo);
            final TailFileState state = tfo.getState();
            tfo.setState(new TailFileState(state.getFilename(), state.getFile(), null, state.getPosition(), state.getTimestamp(), state.getLength(), state.getChecksum(), state.getBuffer()));
        }
    }

    private void cleanReader(TailFileObject tfo) {
        if (tfo.getState() == null) {
            return;
        }

        final FileChannel reader = tfo.getState().getReader();
        if (reader == null) {
            return;
        }

        try {
            reader.close();
            getLogger().debug("Closed FileChannel {}", new Object[]{reader});
        } catch (final IOException ioe) {
            getLogger().warn("Failed to close file handle during cleanup");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if(isMultiChanging.get()) {
            long timeSinceLastLookup = new Date().getTime() - lastLookup.get();
            if(timeSinceLastLookup > context.getProperty(LOOKUP_FREQUENCY).asTimePeriod(TimeUnit.MILLISECONDS)) {
                try {
                    recoverState(context);
                } catch (IOException e) {
                    getLogger().error("Exception raised while looking up for new files", e);
                    context.yield();
                    return;
                }
            }
        }
        if(states.isEmpty()) {
            context.yield();
            return;
        }
        for (String tailFile : states.keySet()) {
            processTailFile(context, session, tailFile);
        }
    }

    private void processTailFile(final ProcessContext context, final ProcessSession session, final String tailFile) {
        // If user changes the file that is being tailed, we need to consume the already-rolled-over data according
        // to the Initial Start Position property
        boolean rolloverOccurred;
        TailFileObject tfo = states.get(tailFile);

        if (tfo.isTailFileChanged()) {
            rolloverOccurred = false;
            final String recoverPosition = context.getProperty(START_POSITION).getValue();

            if (START_BEGINNING_OF_TIME.getValue().equals(recoverPosition)) {
                recoverRolledFiles(context, session, tailFile, tfo.getExpectedRecoveryChecksum(), tfo.getState().getTimestamp(), tfo.getState().getPosition());
            } else if (START_CURRENT_FILE.getValue().equals(recoverPosition)) {
                cleanup();
                tfo.setState(new TailFileState(tailFile, null, null, 0L, 0L, 0L, null, tfo.getState().getBuffer()));
            } else {
                final String filename = tailFile;
                final File file = new File(filename);

                try {
                    final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                    getLogger().debug("Created FileChannel {} for {}", new Object[]{fileChannel, file});

                    final Checksum checksum = new CRC32();
                    final long position = file.length();
                    final long timestamp = file.lastModified();

                    try (final InputStream fis = new FileInputStream(file);
                            final CheckedInputStream in = new CheckedInputStream(fis, checksum)) {
                        StreamUtils.copy(in, new NullOutputStream(), position);
                    }

                    fileChannel.position(position);
                    cleanup();
                    tfo.setState(new TailFileState(filename, file, fileChannel, position, timestamp, file.length(), checksum, tfo.getState().getBuffer()));
                } catch (final IOException ioe) {
                    getLogger().error("Attempted to position Reader at current position in file {} but failed to do so due to {}", new Object[]{file, ioe.toString()}, ioe);
                    context.yield();
                    return;
                }
            }

            tfo.setTailFileChanged(false);
        } else {
            // Recover any data that may have rolled over since the last time that this processor ran.
            // If expectedRecoveryChecksum != null, that indicates that this is the first iteration since processor was started, so use whatever checksum value
            // was present when the state was last persisted. In this case, we must then null out the value so that the next iteration won't keep using the "recovered"
            // value. If the value is null, then we know that either the processor has already recovered that data, or there was no state persisted. In either case,
            // use whatever checksum value is currently in the state.
            Long expectedChecksumValue = tfo.getExpectedRecoveryChecksum();
            if (expectedChecksumValue == null) {
                expectedChecksumValue = tfo.getState().getChecksum() == null ? null : tfo.getState().getChecksum().getValue();
            }

            rolloverOccurred = recoverRolledFiles(context, session, tailFile, expectedChecksumValue, tfo.getState().getTimestamp(), tfo.getState().getPosition());
            tfo.setExpectedRecoveryChecksum(null);
        }

        // initialize local variables from state object; this is done so that we can easily change the values throughout
        // the onTrigger method and then create a new state object after we finish processing the files.
        TailFileState state = tfo.getState();
        File file = state.getFile();
        FileChannel reader = state.getReader();
        Checksum checksum = state.getChecksum();
        if (checksum == null) {
            checksum = new CRC32();
        }
        long position = state.getPosition();
        long timestamp = state.getTimestamp();
        long length = state.getLength();

        // Create a reader if necessary.
        if (file == null || reader == null) {
            file = new File(tailFile);
            reader = createReader(file, position);
            if (reader == null) {
                context.yield();
                return;
            }
        }

        final long startNanos = System.nanoTime();

        // Check if file has rotated
        if (rolloverOccurred
                || (timestamp <= file.lastModified() && length > file.length())) {

            // Since file has rotated, we close the reader, create a new one, and then reset our state.
            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", new Object[]{reader, reader});
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close reader for {} due to {}", new Object[]{file, ioe});
            }

            reader = createReader(file, 0L);
            position = 0L;
            checksum.reset();
        }

        if (file.length() == position || !file.exists()) {
            // no data to consume so rather than continually running, yield to allow other processors to use the thread.
            getLogger().debug("No data to consume; created no FlowFiles");
            tfo.setState(new TailFileState(tailFile, file, reader, position, timestamp, length, checksum, state.getBuffer()));
            persistState(tfo, context);
            context.yield();
            return;
        }

        // If there is data to consume, read as much as we can.
        final TailFileState currentState = state;
        final Checksum chksum = checksum;
        // data has been written to file. Stream it to a new FlowFile.
        FlowFile flowFile = session.create();

        final FileChannel fileReader = reader;
        final AtomicLong positionHolder = new AtomicLong(position);
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

            final Map<String, String> attributes = new HashMap<>(3);
            attributes.put(CoreAttributes.FILENAME.key(), flowFileName);
            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            attributes.put("tailfile.original.path", tailFile);
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
            length = file.length();
            getLogger().debug("Created {} and routed to success", new Object[]{flowFile});
        }

        // Create a new state object to represent our current position, timestamp, etc.
        tfo.setState(new TailFileState(tailFile, file, reader, position, timestamp, length, checksum, state.getBuffer()));

        // We must commit session before persisting state in order to avoid data loss on restart
        session.commit();
        persistState(tfo, context);
    }

    /**
     * Read new lines from the given FileChannel, copying it to the given Output
     * Stream. The Checksum is used in order to later determine whether or not
     * data has been consumed.
     *
     * @param reader The FileChannel to read data from
     * @param buffer the buffer to use for copying data
     * @param out the OutputStream to copy the data to
     * @param checksum the Checksum object to use in order to calculate checksum
     * for recovery purposes
     *
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(final FileChannel reader, final ByteBuffer buffer, final OutputStream out, final Checksum checksum) throws IOException {
        getLogger().debug("Reading lines starting at position {}", new Object[]{reader.position()});

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
                        case '\n': {
                            baos.write(ch);
                            seenCR = false;
                            baos.writeTo(out);
                            final byte[] baosBuffer = baos.toByteArray();
                            checksum.update(baosBuffer, 0, baos.size());
                            if (getLogger().isTraceEnabled()) {
                                getLogger().trace("Checksum updated to {}", new Object[]{checksum.getValue()});
                            }

                            baos.reset();
                            rePos = pos + i + 1;
                            linesRead++;
                            break;
                        }
                        case '\r': {
                            baos.write(ch);
                            seenCR = true;
                            break;
                        }
                        default: {
                            if (seenCR) {
                                seenCR = false;
                                baos.writeTo(out);
                                final byte[] baosBuffer = baos.toByteArray();
                                checksum.update(baosBuffer, 0, baos.size());
                                if (getLogger().isTraceEnabled()) {
                                    getLogger().trace("Checksum updated to {}", new Object[]{checksum.getValue()});
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
                }

                pos = reader.position();
            }

            if (rePos < reader.position()) {
                getLogger().debug("Read {} lines; repositioning reader from {} to {}", new Object[]{linesRead, pos, rePos});
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
     * <li>The Last Modified Time on the file is equal to or later than the
     * given minimum timestamp</li>
     * </ul>
     *
     * <p>
     * The List that is returned will be ordered by file timestamp, providing
     * the oldest file first.
     * </p>
     *
     * @param context the ProcessContext to use in order to determine Processor
     * configuration
     * @param minTimestamp any file with a Last Modified Time before this
     * timestamp will not be returned
     * @return a list of all Files that have rolled over
     * @throws IOException if unable to perform the listing of files
     */
    private List<File> getRolledOffFiles(final ProcessContext context, final long minTimestamp, final String tailFilePath) throws IOException {
        final File tailFile = new File(tailFilePath);
        File directory = tailFile.getParentFile();
        if (directory == null) {
            directory = new File(".");
        }

        String rollingPattern = context.getProperty(ROLLING_FILENAME_PATTERN).getValue();
        if (rollingPattern == null) {
            return Collections.emptyList();
        } else {
            rollingPattern = rollingPattern.replace("${filename}", StringUtils.substringBeforeLast(tailFile.getName(), "."));
        }

        final List<File> rolledOffFiles = new ArrayList<>();
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory.toPath(), rollingPattern)) {
            for (final Path path : dirStream) {
                final File file = path.toFile();
                final long lastMod = file.lastModified();

                if (file.lastModified() < minTimestamp) {
                    getLogger().debug("Found rolled off file {} but its last modified timestamp is before the cutoff (Last Mod = {}, Cutoff = {}) so will not consume it",
                            new Object[]{file, lastMod, minTimestamp});

                    continue;
                } else if (file.equals(tailFile)) {
                    continue;
                }

                rolledOffFiles.add(file);
            }
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

    private Scope getStateScope(final ProcessContext context) {
        final String location = context.getProperty(STATE_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    private void persistState(final TailFileObject tfo, final ProcessContext context) {
        persistState(tfo.getState().toStateMap(tfo.getFilenameIndex()), context);
    }

    private void persistState(final Map<String, String> state, final ProcessContext context) {
        try {
            StateMap oldState = context.getStateManager().getState(getStateScope(context));
            Map<String, String> updatedState = new HashMap<String, String>();

            for(String key : oldState.toMap().keySet()) {
                // These states are stored by older version of NiFi, and won't be used anymore.
                // New states have 'file.<index>.' prefix.
                if (TailFileState.StateKeys.CHECKSUM.equals(key)
                        || TailFileState.StateKeys.FILENAME.equals(key)
                        || TailFileState.StateKeys.POSITION.equals(key)
                        || TailFileState.StateKeys.TIMESTAMP.equals(key)) {
                    getLogger().info("Removed state {}={} stored by older version of NiFi.", new Object[]{key, oldState.get(key)});
                    continue;
                }
                updatedState.put(key, oldState.get(key));
            }

            updatedState.putAll(state);
            context.getStateManager().setState(updatedState, getStateScope(context));
        } catch (final IOException e) {
            getLogger().warn("Failed to store state due to {}; some data may be duplicated on restart of NiFi", new Object[]{e});
        }
    }

    private FileChannel createReader(final File file, final long position) {
        final FileChannel reader;

        try {
            reader = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (final IOException ioe) {
            getLogger().warn("Unable to open file {}; will attempt to access file again after the configured Yield Duration has elapsed: {}", new Object[]{file, ioe});
            return null;
        }

        getLogger().debug("Created FileChannel {} for {}", new Object[]{reader, file});

        try {
            reader.position(position);
        } catch (final IOException ioe) {
            getLogger().error("Failed to read from {} due to {}", new Object[]{file, ioe});

            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", new Object[]{reader});
            } catch (final IOException ioe2) {
            }

            return null;
        }

        return reader;
    }

    // for testing purposes
    Map<String, TailFileObject> getState() {
        return states;
    }

    /**
     * Finds any files that have rolled over and have not yet been ingested by
     * this Processor. Each of these files that is found will be ingested as its
     * own FlowFile. If a file is found that has been partially ingested, the
     * rest of the file will be ingested as a single FlowFile but the data that
     * already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor
     * configuration.
     * @param session the ProcessSession to use in order to interact with
     * FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the
     * oldest file from offset 0 through &lt;position&gt;.
     * @param timestamp the latest Last Modified Timestamp that has been
     * consumed. Any data that was written before this data will not be
     * ingested.
     * @param position the byte offset in the file being tailed, where tailing
     * last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over,
     * <code>false</code> otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final String tailFile, final Long expectedChecksum, final long timestamp, final long position) {
        try {
            // Find all files that match our rollover pattern, if any, and order them based on their timestamp and filename.
            // Ignore any file that has a timestamp earlier than the state that we have persisted. If we were reading from
            // a file when we stopped running, then that file that we were reading from should be the first file in this list,
            // assuming that the file still exists on the file system.
            final List<File> rolledOffFiles = getRolledOffFiles(context, timestamp, tailFile);
            return recoverRolledFiles(context, session, tailFile, rolledOffFiles, expectedChecksum, timestamp, position);
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over due to {}", new Object[]{e});
            return false;
        }
    }

    /**
     * Finds any files that have rolled over and have not yet been ingested by
     * this Processor. Each of these files that is found will be ingested as its
     * own FlowFile. If a file is found that has been partially ingested, the
     * rest of the file will be ingested as a single FlowFile but the data that
     * already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor
     * configuration.
     * @param session the ProcessSession to use in order to interact with
     * FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the
     * oldest file from offset 0 through &lt;position&gt;.
     * @param timestamp the latest Last Modfiied Timestamp that has been
     * consumed. Any data that was written before this data will not be
     * ingested.
     * @param position the byte offset in the file being tailed, where tailing
     * last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over, false
     * otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final String tailFile, final List<File> rolledOffFiles, final Long expectedChecksum,
            final long timestamp, final long position) {
        try {
            getLogger().debug("Recovering Rolled Off Files; total number of files rolled off = {}", new Object[]{rolledOffFiles.size()});
            TailFileObject tfo = states.get(tailFile);

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
                            getLogger().debug("Checksum for {} matched expected checksum. Will skip first {} bytes", new Object[]{firstFile, position});

                            // This is the same file that we were reading when we shutdown. Start reading from this point on.
                            rolledOffFiles.remove(0);
                            FlowFile flowFile = session.create();
                            flowFile = session.importFrom(in, flowFile);
                            if (flowFile.getSize() == 0L) {
                                session.remove(flowFile);
                                // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
                                cleanup();
                                tfo.setState(new TailFileState(tailFile, null, null, 0L, firstFile.lastModified() + 1L, firstFile.length(), null, tfo.getState().getBuffer()));
                            } else {
                                final Map<String, String> attributes = new HashMap<>(3);
                                attributes.put(CoreAttributes.FILENAME.key(), firstFile.getName());
                                attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
                                attributes.put("tailfile.original.path", tailFile);
                                flowFile = session.putAllAttributes(flowFile, attributes);

                                session.getProvenanceReporter().receive(flowFile, firstFile.toURI().toString(), "FlowFile contains bytes 0 through " + position + " of source file",
                                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                                session.transfer(flowFile, REL_SUCCESS);
                                getLogger().debug("Created {} from rolled over file {} and routed to success", new Object[]{flowFile, firstFile});

                                // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
                                cleanup();
                                tfo.setState(new TailFileState(tailFile, null, null, 0L, firstFile.lastModified() + 1L, firstFile.length(), null, tfo.getState().getBuffer()));

                                // must ensure that we do session.commit() before persisting state in order to avoid data loss.
                                session.commit();
                                persistState(tfo, context);
                            }
                        } else {
                            getLogger().debug("Checksum for {} did not match expected checksum. Checksum for file was {} but expected {}. Will consume entire file",
                                    new Object[]{firstFile, checksumResult, expectedChecksum});
                        }
                    }
                }
            }

            // For each file that we found that matches our Rollover Pattern, and has a last modified date later than the timestamp
            // that we recovered from the state file, we need to consume the entire file. The only exception to this is the file that
            // we were reading when we last stopped, as it may already have been partially consumed. That is taken care of in the
            // above block of code.
            for (final File file : rolledOffFiles) {
                tfo.setState(consumeFileFully(file, context, session, tfo));
            }

            return rolloverOccurred;
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over due to {}", new Object[]{e});
            return false;
        }
    }

    /**
     * Creates a new FlowFile that contains the entire contents of the given
     * file and transfers that FlowFile to success. This method will commit the
     * given session and emit an appropriate Provenance Event.
     *
     * @param file the file to ingest
     * @param context the ProcessContext
     * @param session the ProcessSession
     * @param tfo the current state
     *
     * @return the new, updated state that reflects that the given file has been
     * ingested.
     */
    private TailFileState consumeFileFully(final File file, final ProcessContext context, final ProcessSession session, TailFileObject tfo) {
        FlowFile flowFile = session.create();
        flowFile = session.importFrom(file.toPath(), true, flowFile);
        if (flowFile.getSize() == 0L) {
            session.remove(flowFile);
        } else {
            final Map<String, String> attributes = new HashMap<>(3);
            attributes.put(CoreAttributes.FILENAME.key(), file.getName());
            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            attributes.put("tailfile.original.path", tfo.getState().getFilename());
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.getProvenanceReporter().receive(flowFile, file.toURI().toString());
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Created {} from {} and routed to success", new Object[]{flowFile, file});

            // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
            cleanup();
            tfo.setState(new TailFileState(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue(), null, null, 0L, file.lastModified() + 1L, file.length(), null,
                    tfo.getState().getBuffer()));

            // must ensure that we do session.commit() before persisting state in order to avoid data loss.
            session.commit();
            persistState(tfo, context);
        }

        return tfo.getState();
    }

    static class TailFileObject {

        private TailFileState state = new TailFileState(null, null, null, 0L, 0L, 0L, null, ByteBuffer.allocate(65536));
        private Long expectedRecoveryChecksum;
        private int filenameIndex;
        private boolean tailFileChanged = true;

        public TailFileObject(int i) {
            this.filenameIndex = i;
        }

        public TailFileObject(int index, Map<String, String> statesMap) {
            this.filenameIndex = index;
            this.tailFileChanged = false;
            final String prefix = MAP_PREFIX + index + '.';
            final String filename = statesMap.get(prefix + TailFileState.StateKeys.FILENAME);
            final long position = Long.valueOf(statesMap.get(prefix + TailFileState.StateKeys.POSITION));
            final long timestamp = Long.valueOf(statesMap.get(prefix + TailFileState.StateKeys.TIMESTAMP));
            final long length = Long.valueOf(statesMap.get(prefix + TailFileState.StateKeys.LENGTH));
            this.state = new TailFileState(filename, new File(filename), null, position, timestamp, length, null, ByteBuffer.allocate(65536));
        }

        public int getFilenameIndex() {
            return filenameIndex;
        }

        public void setFilenameIndex(int filenameIndex) {
            this.filenameIndex = filenameIndex;
        }

        public TailFileState getState() {
            return state;
        }

        public void setState(TailFileState state) {
            this.state = state;
        }

        public Long getExpectedRecoveryChecksum() {
            return expectedRecoveryChecksum;
        }

        public void setExpectedRecoveryChecksum(Long expectedRecoveryChecksum) {
            this.expectedRecoveryChecksum = expectedRecoveryChecksum;
        }

        public boolean isTailFileChanged() {
            return tailFileChanged;
        }

        public void setTailFileChanged(boolean tailFileChanged) {
            this.tailFileChanged = tailFileChanged;
        }

    }

    /**
     * A simple Java class to hold information about our state so that we can
     * maintain this state across multiple invocations of the Processor
     */
    static class TailFileState {

        private final String filename; // hold onto filename and not just File because we want to match that against the user-defined filename to recover from
        private final File file;
        private final FileChannel reader;
        private final long position;
        private final long timestamp;
        private final long length;
        private final Checksum checksum;
        private final ByteBuffer buffer;

        private static class StateKeys {
            public static final String FILENAME = "filename";
            public static final String POSITION = "position";
            public static final String TIMESTAMP = "timestamp";
            public static final String CHECKSUM = "checksum";
            public static final String LENGTH = "length";
        }

        public TailFileState(final String filename, final File file, final FileChannel reader,
                final long position, final long timestamp, final long length, final Checksum checksum, final ByteBuffer buffer) {
            this.filename = filename;
            this.file = file;
            this.reader = reader;
            this.position = position;
            this.length = length;
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

        public long getLength() {
            return length;
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

        public Map<String, String> toStateMap(int index) {
            final String prefix = MAP_PREFIX + index + '.';
            final Map<String, String> map = new HashMap<>(4);
            map.put(prefix + StateKeys.FILENAME, filename);
            map.put(prefix + StateKeys.POSITION, String.valueOf(position));
            map.put(prefix + StateKeys.LENGTH, String.valueOf(length));
            map.put(prefix + StateKeys.TIMESTAMP, String.valueOf(timestamp));
            map.put(prefix + StateKeys.CHECKSUM, checksum == null ? null : String.valueOf(checksum.getValue()));
            return map;
        }
    }
}
