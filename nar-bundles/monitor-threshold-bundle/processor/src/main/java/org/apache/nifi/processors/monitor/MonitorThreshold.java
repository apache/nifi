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
package org.apache.nifi.processors.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.file.FileUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.annotation.TriggerWhenEmpty;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;

/**
 * This processor has an advanced user interface that is used to specify
 * attributes that should be monitored, as well as thresholds for individual
 * values that are expected to appear in the monitored attributes.
 */

/* Because this processor maintains counts (in memory) that must be persisted
 * to disk according to a prescribed schedule (See OPT_COUNT_RESET_TIME/OPT_COUNT_RESET_MINUTES 
 * properties), this processor must be timer driven. This processor cannot be event driven.
 * Using @TriggerWhenEmpty ensures that the processor will be timer driven, and that its
 * onTrigger method will be called even when no Flowfiles are in an incoming queue. 
 */
@TriggerWhenEmpty
@Tags({"monitor", "threshold", "throttle", "limit", "counts", "size", "bytes", "files", "attribute", "values", "notify"})
@CapabilityDescription("Examines values found in selected FlowFile attributes and "
        + "maintains counts of the number of times the values have been encountered."
        + "The counts are then used to check against user defined thresholds.")
public class MonitorThreshold extends AbstractProcessor {

    // default property values
    public static final String DEFAULT_THRESHOLDS_KEY = "Default";
    public static final String DEFAULT_COUNTS_PERSISTENCE_FILE_PREFIX = "conf/MonitorThreshold";
    public static final int DEFAULT_SAVE_COUNTS_FREQ_SECS = 30;
    public static final String DEFAULT_DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES = ".";
    public static final String DEFAULT_ADD_ATRIBUTES_WHEN_THRESHOLD_EXCEEDED = "true";

    public static final String ALWAYS = "Always";
    public static final String ONLY_WHEN_THRESHOLD_EXCEEDED = "Only When Threshold Exceeded";
    public static final String NEVER = "Never";
    public static final String DEFAULT_ADD_ATRIBUTES = ONLY_WHEN_THRESHOLD_EXCEEDED;

    // constants/variables, used for clearing/persisting counts
    public static final long MILLIS_IN_MINUTE = 60000L;
    public static final long MILLIS_IN_HOUR = MILLIS_IN_MINUTE * 60;
    public static final String TIME_REGEX = "^((?:[0-1]?[0-9])|(?:[2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])";
    public static final Pattern TIME_PATTERN = Pattern.compile(TIME_REGEX);

    private Long timeCountsLastCleared = System.currentTimeMillis();
    private Long timeCountsLastSaved = null;

    // other constants
    public static final String NUM_APPLICABLE_THRESHOLDS_KEY = "numApplicableThresholds";
    public static final String NUM_THRESHOLDS_EXCEEDED_KEY = "numThresholdsExceeded";

    public static final String FILE_COUNT_PREFIX = "fileCount";
    public static final String FILE_THRESHOLD_PREFIX = "fileThreshold";

    public static final String BYTE_COUNT_PREFIX = "byteCount";
    public static final String BYTE_THRESHOLD_PREFIX = "byteThreshold";

    public static final String AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED_PROPERTY = "Aggregate Counts When No Threshold Provided";
    public static final String COUNT_RESET_TIME_PROPERTY = "Count Reset Time";
    public static final String MINUTES_TO_WAIT_BEFORE_RESETTING_COUNTS_PROPERTY = "Minutes to Wait Before Resetting Counts";
    public static final String ADD_ATTRIBUTES_PROPERTY = "Add Attributes";
    public static final String MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY = "Maximum Attribute Pairs to Add When Multiple Thresholds Exceeded";
    public static final String ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES_PROPERTY = "Attribute to use for Counting Bytes";
    public static final String DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES_PROPERTY = "Delimiter to use for Counts Attributes";
    public static final String STANDARD_FLOWFILE_FILESIZE_PROPERTY = "fileSize";
    public static final String FREQUENCY_TO_SAVE_COUNTS_PROPERTY = "Frequency to Save Counts (seconds)";
    public static final String PREFIX_FOR_COUNTS_PERSISTENCE_FILE_PROPERTY = "Prefix for Counts Persistence File";

    // locks for concurrency protection
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    // file and byte counts, etc...
    private ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> counts = null; // Guarded by rwLock
    private Map<String, Map<String, CompareRecord>> thresholds; // Guarded by rwLock

    // optional properties
    private List<PropertyDescriptor> properties;

    public static final PropertyDescriptor OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED = new PropertyDescriptor.Builder()
            .name(AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED_PROPERTY)
            .description("When a value (in a monitored attribute) is encountered, but no thresholds have been specified "
                    + "for that value, how should counts be maintained? "
                    + "If this property is true (default), two behaviors are adopted.  "
                    + "First, counts for ALL non-threshold values are aggregated into a single count. "
                    + "Second, when a non-threshold value is encountered, the aggregate count will be compared to the default threshold."
                    + "If this property is false, the behaviors change, as follows.  "
                    + "Separate, individual counts are maintained for each non-threshold, value that is encountered. "
                    + "When a non-threshold value is encountered, it's unique count will be compared with the default threshold. "
                    + "NOTE: Counts are never maintained for non-monitored attributes. ")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_COUNT_RESET_TIME = new PropertyDescriptor.Builder()
            .name(COUNT_RESET_TIME_PROPERTY)
            .description("The time (24-hour clock, hh:mm:ss format, GMT Timezone) when the byte and file counts will be reset to zero.  "
                    + "Note: To prevent counts from increasing forever, you must specify either a '" + COUNT_RESET_TIME_PROPERTY + "' or '" + MINUTES_TO_WAIT_BEFORE_RESETTING_COUNTS_PROPERTY + "'")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(TIME_PATTERN))
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_COUNT_RESET_MINUTES = new PropertyDescriptor.Builder()
            .name(MINUTES_TO_WAIT_BEFORE_RESETTING_COUNTS_PROPERTY)
            .description("Minutes to delay count reset, beginning at '" + COUNT_RESET_TIME_PROPERTY + "', if provided. "
                    + "If '" + COUNT_RESET_TIME_PROPERTY + "' is not provided, then the minutes to delay are counted from the last reset.  "
                    + "(Last reset is initially the time when the processor is created (added to a flow).)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_ADD_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name(ADD_ATTRIBUTES_PROPERTY)
            .description("Setting this property to'" + ONLY_WHEN_THRESHOLD_EXCEEDED + "' will cause two additional attributes to be added to"
                    + "FlowFiles for every threshold that is exceeded.  For example, if a file count threshold is exceeded, "
                    + " a fileCount.attributeName.value and a fileThreshold.attributeName.value will be added to the FlowFile. "
                    + "Setting this property to'" + ALWAYS + "' will cause attributes to be added, no matter whether a threshold is exceeded."
                    + "Note 1: Attributes are only added if a monitored attribute is present on the FlowFile, no matter if '" + "' " + ADD_ATTRIBUTES_PROPERTY + "' is set to '" + ALWAYS + "'. "
                    + "Note 2: This processor makes a log entry for every threshold that is exceeded, no matter the value of this property. ")
            .required(false)
            .defaultValue(DEFAULT_ADD_ATRIBUTES)
            .allowableValues(ALWAYS, ONLY_WHEN_THRESHOLD_EXCEEDED, NEVER)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_MAX_ATTRIBUTE_PAIRS_TO_ADD_WHEN_MULTIPLE_THRESHOLD_EXCEEDED = new PropertyDescriptor.Builder()
            .name(MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY)
            .description("Controls/limits the number of FlowFile attributes that are added when "
                    + "multiple thresholds are exceeded.  Recall that when a threshold is exceeded, "
                    + "a pair of two attributes are added to the FlowFile, one is the current count and "
                    + "the other is the threshold.  If 100 thresholds are exceeded, then 200 "
                    + "attributes will be added to the FlowFile.  Setting this property to zero means add "
                    + "all count/threshold pairs that were exceeded.  Any setting greater than zero indicates "
                    + "how many count/threshold pairs to add when multiple thresholds are exceeded.  "
                    + "Only non-negative settings are supported.  The default is 0.  "
                    + "In effect only when '" + ADD_ATTRIBUTES_PROPERTY + "' is '" + ONLY_WHEN_THRESHOLD_EXCEEDED + "'."
                    + "Setting " + ADD_ATTRIBUTES_PROPERTY + " to" + " '" + ALWAYS + "' or '" + NEVER
                    + "' causes '" + MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY + "' to be ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name(DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES_PROPERTY)
            .description("The delimiter to use for naming threshold exceeded attributes,"
                    + "e.g., fileCount.attributeName.value.  Defaults to \'.\'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES = new PropertyDescriptor.Builder()
            .name(ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES_PROPERTY)
            .description("Setting this property allows a FlowFile attribute to be used for counting bytes, "
                    + "in place of the actual fileSize.  Note that the attribute named by this property "
                    + "must contain only numeric, non-negative integer values for each FlowFile. "
                    + "Non-numeric or negative values that are encountered will be ignored, causing the actual fileSize "
                    + "to be used instead.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(STANDARD_FLOWFILE_FILESIZE_PROPERTY)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_FREQUENCY_TO_SAVE_COUNTS_SECS = new PropertyDescriptor.Builder()
            .name(FREQUENCY_TO_SAVE_COUNTS_PROPERTY)
            .description("How often the counts should be written to disk. The default value is 30 seconds.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("30")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor OPT_COUNTS_PERSISTENCE_FILE_PREFIX = new PropertyDescriptor.Builder()
            .name(PREFIX_FOR_COUNTS_PERSISTENCE_FILE_PROPERTY)
            .description("The prefix for the file that is saved (to disk) to maintain counts across NIFI restarts. "
                    + "By default, the value is " + DEFAULT_COUNTS_PERSISTENCE_FILE_PREFIX + ". The actual name of the counts file will "
                    + "be this value plus \"-XXXX.counts\" where XXXX is the processor ID.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_COUNTS_PERSISTENCE_FILE_PREFIX)
            .expressionLanguageSupported(false)
            .build();

    // relationships
    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    public static final String SUCCESS = "success";
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name(SUCCESS)
            .description("All FlowFiles follow this relationship, unless there is a problem with the FlowFile.")
            .build();

    public static final String FAILURE = "failure";
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name(FAILURE)
            .description("FlowFiles follow this relationship path if there is a problem with the FlowFile, or if there is a unrecoverable configuration error.")
            .build();

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> problems = new ArrayList<>();

        String countResetTime = validationContext.getProperty(OPT_COUNT_RESET_TIME).getValue();
        String minutesToWaitBeforeResettingCounts = validationContext.getProperty(OPT_COUNT_RESET_MINUTES).getValue();

        if (countResetTime == null && minutesToWaitBeforeResettingCounts == null) {

            ValidationResult rolloverTimeOrMinutesRequired = new ValidationResult.Builder()
                    .subject("Invalid processor configuration.")
                    .explanation("To prevent counts from increasing forever, you must specify either a '" + COUNT_RESET_TIME_PROPERTY + "' or '" + MINUTES_TO_WAIT_BEFORE_RESETTING_COUNTS_PROPERTY + "'.")
                    .input(COUNT_RESET_TIME_PROPERTY + ": " + countResetTime + " "
                            + MINUTES_TO_WAIT_BEFORE_RESETTING_COUNTS_PROPERTY + ": " + minutesToWaitBeforeResettingCounts)
                    .valid(false)
                    .build();

            problems.add(rolloverTimeOrMinutesRequired);
        }

        int maxPairsToAdd = Integer.parseInt(validationContext.getProperty(OPT_MAX_ATTRIBUTE_PAIRS_TO_ADD_WHEN_MULTIPLE_THRESHOLD_EXCEEDED).getValue());
        if (maxPairsToAdd < 0) {
            ValidationResult maxPairsToAddCannotBeNegative = new ValidationResult.Builder()
                    .subject("Invalid processor configuration.")
                    .explanation(MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY + " cannot be negative.")
                    .input(MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY + ": " + maxPairsToAdd)
                    .valid(false)
                    .build();

            problems.add(maxPairsToAddCannotBeNegative);
        }
        return problems;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED);
        properties.add(OPT_COUNT_RESET_TIME);
        properties.add(OPT_COUNT_RESET_MINUTES);
        properties.add(OPT_ADD_ATTRIBUTES);
        properties.add(OPT_MAX_ATTRIBUTE_PAIRS_TO_ADD_WHEN_MULTIPLE_THRESHOLD_EXCEEDED);
        properties.add(OPT_DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES);
        properties.add(OPT_ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES);
        properties.add(OPT_FREQUENCY_TO_SAVE_COUNTS_SECS);
        properties.add(OPT_COUNTS_PERSISTENCE_FILE_PREFIX);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        relationships.add(RELATIONSHIP_FAILURE);
        this.relationships.set(Collections.unmodifiableSet(relationships));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @OnStopped
    public void processorTasksStopped() {
        writeLock.lock();
        try {
            // Since the user can change thresholds when the processor is stopped,
            //	force the user thresholds to be reloaded on the next call to onTrigger()
            this.thresholds = null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();
        //ProvenanceReporter provenanceReporter = session.getProvenanceReporter();

        getCounts(context, session, logger);

        if (shouldClearCounts(context)) {
            clearCounts(context, session, logger);
        }

        if (shouldSaveCounts(context)) {
            saveCounts(getCountsFile(context), logger);
        }

        FlowFile flowFile = null;
        try {
            final Map<String, Map<String, CompareRecord>> thresholds = getThresholds(context, logger);
            if (thresholds == null) {
                logger.error(this + " unable to read thresholds.");
                return;
            }

            flowFile = session.get();
            if (flowFile == null) {
                return;
            }

            // prep to loop through all thresholds for this FlowFile
            final long fileSize = getFileSize(context, logger, flowFile);
            final String delimiter = context.getProperty(OPT_DELIMITER_TO_USE_FOR_COUNTS_ATTRIBUTES).getValue();

            boolean aggregateCountsWhenNoThresholdProvided = Boolean.parseBoolean(context.getProperty(OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED).getValue());
            String addAttributes = context.getProperty(OPT_ADD_ATTRIBUTES).getValue();
            int maxPairsToAdd = Integer.parseInt(context.getProperty(OPT_MAX_ATTRIBUTE_PAIRS_TO_ADD_WHEN_MULTIPLE_THRESHOLD_EXCEEDED).getValue());
            int countOfPairsAdded = 0;
            final List<String> reasonsThresholdsExceeded = new ArrayList<>();
            long numApplicableThresholds = 0, numThresholdsExceeded = 0;

            // loop through all thresholds for this FlowFile
            for (final String attributeName : thresholds.keySet()) { //user configured thresholds
                String attributeValue = flowFile.getAttribute(attributeName);
                if (attributeValue == null) { // attribute does not exist on this FlowFile
                    continue; // check for any remaining thresholds that might apply to attributes on this FlowFile
                }

                CompareRecord threshold = thresholds.get(attributeName).get(attributeValue);  //has (user configured) thresholds only, not actual counts
                CompareRecord countsForAttrValue = null;
                if (threshold == null) { //user did not specify a threshold for the attributeValue found in the FlowFile
                    threshold = thresholds.get(attributeName).get(DEFAULT_THRESHOLDS_KEY); //use default thresholds
                    if (aggregateCountsWhenNoThresholdProvided) {
                        // ensure there is a default count record
                        countsForAttrValue = getCountsForAttributeValue(getCountsForAttribute(attributeName), DEFAULT_THRESHOLDS_KEY, attributeName, threshold); // if no default count record available, one will be created
                    } else {
                        countsForAttrValue = getCountsForAttributeValue(getCountsForAttribute(attributeName), attributeValue, attributeName, threshold); // if no count record available, one will be created
                    }
                } else { // there is a user specified threshold
                    countsForAttrValue = getCountsForAttributeValue(getCountsForAttribute(attributeName), attributeValue, attributeName, threshold); // if no count record available, one will be created	            	
                }
                // update internal (processor) counts
                countsForAttrValue.addToFlowFileCounts(fileSize);

                String attributeNameAndValue = delimiter + countsForAttrValue.getAttributeName() + delimiter + attributeValue;
                if (addAttributes.equals(ALWAYS)) {
                    flowFile = session.putAttribute(flowFile, FILE_COUNT_PREFIX + attributeNameAndValue, Integer.toString(countsForAttrValue.getFileCount()));
                    flowFile = session.putAttribute(flowFile, FILE_THRESHOLD_PREFIX + attributeNameAndValue, Integer.toString(countsForAttrValue.getFileThreshold()));
                    countOfPairsAdded++;
                    flowFile = session.putAttribute(flowFile, BYTE_COUNT_PREFIX + attributeNameAndValue, Long.toString(countsForAttrValue.getByteCount()));
                    flowFile = session.putAttribute(flowFile, BYTE_THRESHOLD_PREFIX + attributeNameAndValue, Long.toString(countsForAttrValue.getByteThreshold()));
                    countOfPairsAdded++;
                }

                // check thresholds
                if (countsForAttrValue.fileThresholdExceeded()) {
                    numThresholdsExceeded++;
                    reasonsThresholdsExceeded.add(countsForAttrValue.getReasonFileLimitExceeded());
                    if (addAttributes.equals(ONLY_WHEN_THRESHOLD_EXCEEDED) && (maxPairsToAdd >= 0) && ((maxPairsToAdd == 0) || (countOfPairsAdded < maxPairsToAdd))) {
                        //String attributeNameAndValue = delimiter + countsForAttrValue.getAttributeName() + delimiter + attributeValue;
                        flowFile = session.putAttribute(flowFile, FILE_COUNT_PREFIX + attributeNameAndValue, Integer.toString(countsForAttrValue.getFileCount()));
                        flowFile = session.putAttribute(flowFile, FILE_THRESHOLD_PREFIX + attributeNameAndValue, Integer.toString(countsForAttrValue.getFileThreshold()));
                        countOfPairsAdded++;
                    }
                }
                if (countsForAttrValue.byteThresholdExceeded()) {
                    numThresholdsExceeded++;
                    reasonsThresholdsExceeded.add(countsForAttrValue.getReasonByteLimitExceeded());
                    if (addAttributes.equals(ONLY_WHEN_THRESHOLD_EXCEEDED) && (maxPairsToAdd >= 0) && ((maxPairsToAdd == 0) || (countOfPairsAdded < maxPairsToAdd))) {
                        //String attributeNameAndValue = delimiter + countsForAttrValue.getAttributeName() + delimiter + attributeValue;
                        flowFile = session.putAttribute(flowFile, BYTE_COUNT_PREFIX + attributeNameAndValue, Long.toString(countsForAttrValue.getByteCount()));
                        flowFile = session.putAttribute(flowFile, BYTE_THRESHOLD_PREFIX + attributeNameAndValue, Long.toString(countsForAttrValue.getByteThreshold()));
                        countOfPairsAdded++;
                    }
                }

                numApplicableThresholds = numApplicableThresholds + 2; // the UI requires that both a file and byte threshold be supplied

            } // end loop through all attributes with thresholds 

            if (numThresholdsExceeded > 0) {
                logLimitsWereExceeded(logger, flowFile, reasonsThresholdsExceeded);
            }

            flowFile = session.putAttribute(flowFile, NUM_APPLICABLE_THRESHOLDS_KEY, Long.toString(numApplicableThresholds));
            flowFile = session.putAttribute(flowFile, NUM_THRESHOLDS_EXCEEDED_KEY, Long.toString(numThresholdsExceeded));

            session.transfer(flowFile, RELATIONSHIP_SUCCESS);

        } catch (final Throwable t) {
            if (flowFile != null) {
                logger.warn("Failed to process " + flowFile + ". Sending to failure relationship.", t);
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
            }
        }
    } //end onTrigger(...)

    private void logLimitsWereExceeded(ProcessorLog logger, final FlowFile flowFile, final List<String> reasonsThresholdExceeded) {
        final StringBuilder sb = new StringBuilder();

        sb.append("Threshold(s) Exceeded for: ").append(flowFile);
        int numberOfReasons = reasonsThresholdExceeded.size();
        for (int i = 0; i < numberOfReasons; i++) {
            sb.append(reasonsThresholdExceeded.get(i));
        }
        logger.info("\n" + sb.toString());
    }

    private long getFileSize(final ProcessContext context, final ProcessorLog logger, FlowFile flowFile) {

        long fileSize = flowFile.getSize();

        String attributeToUseForCountingBytes = context.getProperty(OPT_ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES).getValue();
        if (attributeToUseForCountingBytes != null) {
            if (attributeToUseForCountingBytes.equals(STANDARD_FLOWFILE_FILESIZE_PROPERTY)) {
                return fileSize;
            }
            final String valueToUseForCountingBytes = flowFile.getAttribute(attributeToUseForCountingBytes);
            if (valueToUseForCountingBytes != null) {
                try {
                    final int intVal = Integer.parseInt(valueToUseForCountingBytes);
                    if (intVal >= 0) {
                        fileSize = intVal;
                    } else {
                        logger.warn(this + " Value to use for counting bytes must be numeric and non-negative.  Found: " + valueToUseForCountingBytes);
                    }
                } catch (final NumberFormatException e) {
                    logger.warn(this + " Value to use for counting bytes must be numeric and non-negative.  Found: " + valueToUseForCountingBytes);
                }
            } else {
                logger.warn(this + " Value to use for counting bytes cannot be null.  It must be numeric and non-negative.  Found: " + valueToUseForCountingBytes);
            }
        } else {
            logger.warn(this + ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES_PROPERTY + " property cannot be empty.");
        }
        return fileSize;
    }

    private ConcurrentHashMap<String, CompareRecord> getCountsForAttribute(final String attributeName) {
        ConcurrentHashMap<String, CompareRecord> countsForThisAttribute = null;
        readLock.lock();
        try {
            countsForThisAttribute = this.counts.get(attributeName);
        } finally {
            readLock.unlock();
        }
        if (countsForThisAttribute == null) { 	// if no count record is available, create one
            countsForThisAttribute = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, CompareRecord> putResult = null;
            writeLock.lock();
            try {
                putResult = this.counts.putIfAbsent(attributeName, countsForThisAttribute);
            } finally {
                writeLock.unlock();
            }
            if (putResult != null) {
                countsForThisAttribute = putResult;
            }
        }
        return countsForThisAttribute;
    }

    private CompareRecord getCountsForAttributeValue(ConcurrentHashMap<String, CompareRecord> countsForThisAttribute,
            final String attributeValue, final String attributeName, final CompareRecord threshold) {

        CompareRecord countsForThisAttributeValue = null;
        readLock.lock();
        try {
            countsForThisAttributeValue = countsForThisAttribute.get(attributeValue);
        } finally {
            readLock.unlock();
        }

        if (countsForThisAttributeValue == null) { 	// if no count record is available, create one
            countsForThisAttributeValue = new CompareRecord(threshold, attributeName, attributeValue);
            CompareRecord returnedState = null;
            writeLock.lock();
            try {
                returnedState = countsForThisAttribute.putIfAbsent(attributeValue, countsForThisAttributeValue);
            } finally {
                writeLock.unlock();
            }
            if (returnedState != null) {
                countsForThisAttributeValue = returnedState;
            }
        }
        return countsForThisAttributeValue;
    }

    protected boolean shouldSaveCounts(ProcessContext context) {
        writeLock.lock();
        try {
            if (this.timeCountsLastSaved == null) {
                this.timeCountsLastSaved = System.currentTimeMillis();
            }

            final String saveSeconds = context.getProperty(OPT_FREQUENCY_TO_SAVE_COUNTS_SECS).getValue();
            final int seconds = saveSeconds == null ? DEFAULT_SAVE_COUNTS_FREQ_SECS : Integer.parseInt(saveSeconds);
            final long timeToSave = seconds * 1000 + this.timeCountsLastSaved;

            return timeToSave <= System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }

    protected void saveCounts(final File file, ProcessorLog logger) {
        readLock.lock();
        try {
            if (this.counts == null) {
                return;
            }

            try {
                final FileOutputStream fos = new FileOutputStream(file);
                final ObjectOutputStream oos = new ObjectOutputStream(fos);
                try {
                    oos.writeObject(this.counts);
                    Long time = this.timeCountsLastCleared;
                    if (time == null) {
                        time = 0L;
                    }
                    oos.writeObject(time);
                } finally {
                    FileUtils.closeQuietly(oos);
                }

                logger.debug(this + " saved current counts to file " + file.getAbsolutePath());
            } catch (final IOException e) {
                logger.error("Unable to save counts to file " + file.getAbsolutePath() + " due to " + e);
            }
        } finally {
            readLock.unlock();
        }

        writeLock.lock();
        try {
            this.timeCountsLastSaved = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }

    protected synchronized ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> getCounts(final ProcessContext context, final ProcessSession session, final ProcessorLog logger) {
        if (this.counts != null) {
            return this.counts;
        }

        final File file = getCountsFile(context);
        if (file.exists()) {
            try {
                final Tuple<Long, ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>> tuple = readCountsFile(file);
                writeLock.lock();
                try {
                    timeCountsLastCleared = tuple.getKey();
                    this.counts = tuple.getValue();
                } finally {
                    writeLock.unlock();
                }

                if (shouldClearCounts(context)) {
                    logger.warn(this + " restored saved counts but the counts have already expired. Clearing counts now.");
                    clearCounts(context, session, logger);
                } else {
                    logger.info(this + " restored saved counts");
                }

                return this.counts;
            } catch (final IOException e) {
                logger.error(this + " unable to read counts from file " + file.getAbsolutePath() + " due to " + e);
                this.counts = new ConcurrentHashMap<>();
            }
        } else {
            this.counts = new ConcurrentHashMap<>();
        }

        return this.counts;
    }

    private File getCountsFile(ProcessContext context) {
        final String countsFilePrefixVal = context.getProperty(OPT_COUNTS_PERSISTENCE_FILE_PREFIX).getValue();
        final String countsFilePrefix = countsFilePrefixVal == null ? DEFAULT_COUNTS_PERSISTENCE_FILE_PREFIX : countsFilePrefixVal;
        final String countsFilename = countsFilePrefix + "-" + this.getIdentifier() + ".counts";

        final File file = new File(countsFilename);
        return file;
    }

    @SuppressWarnings("unchecked")
    protected Tuple<Long, ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>> readCountsFile(final File file)
            throws IOException {
        final FileInputStream fis = new FileInputStream(file);
        final ObjectInputStream ois = new ObjectInputStream(fis);
        try {
            final Object mapObject = ois.readObject();
            final Long rolloverTime = (Long) ois.readObject();
            return new Tuple<>(rolloverTime, (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) mapObject);
        } catch (final ClassNotFoundException e) {
            throw new IOException(e);
        } finally {
            FileUtils.closeQuietly(ois);
        }
    }

    private Map<String, Map<String, CompareRecord>> getThresholds(ProcessContext context, ProcessorLog logger) {
        Map<String, Map<String, CompareRecord>> thresholds;
        readLock.lock();
        try {
            if (this.thresholds == null || this.thresholds.isEmpty()) {
                thresholds = null;
            } else {
                thresholds = new HashMap<>(this.thresholds);
            }
        } finally {
            readLock.unlock();
        }

        if (thresholds == null) {
            writeLock.lock();
            try {

                if (this.thresholds == null) {
                    final ThresholdsParser parser = new ThresholdsParser(logger);
                    this.thresholds = parser.readThresholds(context.getAnnotationData()); //read thresholds from flow.xml file 
                }

                thresholds = Collections.unmodifiableMap(this.thresholds);
            } catch (final Exception e) {
                logger.error("Unable to read Thresholds.", e);
                return null;
            } finally {
                writeLock.unlock();
            }
        }

        return thresholds;
    }

    protected boolean shouldClearCounts(ProcessContext context) {
        readLock.lock();
        try {
            // If we are rolling over based on time, rather than a certain number of minutes,
            // then we don't want to allow the millisToClear to be greater than 24 hours.
            final boolean usingTime = (context.getProperty(OPT_COUNT_RESET_TIME).getValue() != null);
            long millisToClear = calculateCountResetMillis(context);
            if (usingTime) {
                millisToClear = Math.min(MILLIS_IN_HOUR * 24, millisToClear);
            }

            long timeToClear = this.timeCountsLastCleared + millisToClear;
            return timeToClear < System.currentTimeMillis();
        } finally {
            readLock.unlock();
        }
    }

    protected long calculateCountResetMillis(ProcessContext context) {
        final String rolloverTimeVal = context.getProperty(OPT_COUNT_RESET_TIME).getValue();
        final String rolloverMinVal = context.getProperty(OPT_COUNT_RESET_MINUTES).getValue();

        if (rolloverTimeVal == null) {
            final int minutes = Integer.parseInt(rolloverMinVal);
            final long millis = minutes * MILLIS_IN_MINUTE;
            return millis;
        } else {
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            final String[] rolloverSplit = rolloverTimeVal.split(":");
            final int rolloverHour = Integer.parseInt(rolloverSplit[0]);
            final int rolloverMinutes = Integer.parseInt(rolloverSplit[1]);
            final int rolloverSeconds = Integer.parseInt(rolloverSplit[2]);

            calendar.set(Calendar.HOUR_OF_DAY, rolloverHour);
            calendar.set(Calendar.MINUTE, rolloverMinutes);
            calendar.set(Calendar.SECOND, rolloverSeconds);
            calendar.set(Calendar.MILLISECOND, 0);

            // Make sure we have the right day.
            final Calendar lastRolloverCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            readLock.lock();
            try {
                lastRolloverCal.setTimeInMillis(this.timeCountsLastCleared);
            } finally {
                readLock.unlock();
            }

            while (calendar.before(lastRolloverCal)) {
                calendar.add(Calendar.DAY_OF_YEAR, 1);
            }

            return calendar.getTimeInMillis() - lastRolloverCal.getTimeInMillis();
        }
    }

    protected void clearCounts(final ProcessContext context, final ProcessSession session, final ProcessorLog logger) {
        writeLock.lock();
        try {
            if (shouldClearCounts(context)) {
                logger.info("\n" + this + " throttling period has elapsed; resetting counts.");
                logCounts(context, logger);
                counts.clear();
                this.timeCountsLastCleared = System.currentTimeMillis();
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected synchronized void logCounts(ProcessContext context, ProcessorLog logger) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n>>>>>>>> MonitorThreshold Report: <<<<<<<<");
        for (String count : this.counts.keySet()) {
            sb.append("\nAttribute with Threshold: ").append(count);
            ConcurrentHashMap<String, CompareRecord> countsForThisAttribute = getCountsForAttribute(count);
            for (String value : countsForThisAttribute.keySet()) {
                sb.append("\nAttribute: ").append(count).append("\tValue: '").append(value).append("'");
                CompareRecord countsForThisAttributeValue = countsForThisAttribute.get(value);
                sb.append("\n\tFiles Threshold:   ").append(countsForThisAttributeValue.getFileThreshold())
                        .append("\tBytes Threshold:    ").append(countsForThisAttributeValue.getByteThreshold())
                        .append("\n\tNum files seen:    ").append(countsForThisAttributeValue.getFileCount())
                        .append("\tNum bytes seen:     ").append(countsForThisAttributeValue.getByteCount());
            }
        }
        sb.append("\n>>>>>> End MonitorThreshold Report. <<<<<<");
        String logReport = sb.toString();
        logger.info(logReport);
    }

    // CompareRecord does double duty.  
    // A single CompareRecord instance is used to contain thresholds (but kept in the count variables).  
    // Other CompareRecord instances contain actual counts, each with a reference back to their 'governing' threshold instance.
    public static class CompareRecord implements Serializable {

        private static final long serialVersionUID = -2759458735761055366L;
        private int fileCount;
        private long byteCount;
        private CompareRecord threshold = null; // reference to the instance containing the thresholds
        private final String attributeName;
        private final String attributeValue;
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Lock compareRecordReadLock = rwLock.readLock();
        private final Lock compareRecordWriteLock = rwLock.writeLock();

        // Used for creating a 'threshold' record.
        public CompareRecord(final String attributeName, final String attributeValue, final int fileThreshold, final long byteThreshold) {
            this.attributeName = attributeName;
            this.attributeValue = attributeValue;
            this.fileCount = fileThreshold; // for threshold records, save thresholds in count fields 
            this.byteCount = byteThreshold; // for threshold records, save thresholds in count fields
        }

        // Used for creating a 'count' record (that is compared against thresholds to determine whether or not a FlowFile exceeds a threshold).
        public CompareRecord(final CompareRecord threshold, final String attributeName, final String attributeValue) {
            this(attributeName, attributeValue, 0, 0); // begin with 0 for the counts
            this.threshold = threshold; // "connect" this count record to its corresponding threshold record
        }

        public void addToFlowFileCounts(final long fileSize) {
            if (threshold == null) {
                throw new IllegalStateException("Attempted to add to counts when no threshold exists!");
            }

            compareRecordWriteLock.lock();
            try {
                byteCount += fileSize;
                fileCount++;

            } finally {
                compareRecordWriteLock.unlock();
            }
        }

        public String getReasonFileLimitExceeded() {
            long fileCnt = getFileCount();
            long fileThreshold = getFileThreshold();
            if (getFileThreshold() < fileCnt) {
                return "\n\tAttribute: '" + attributeName + "' Value '" + attributeValue + "'"
                        + "\tFile threshold: " + fileThreshold
                        + "\tExpected Count: " + fileCnt + " (counting this file)"
                        + ((fileThreshold < fileCnt) ? "\t[threshold exceeded]" : "\t[below thresholds]");
            }
            return null;
        }

        public String getReasonByteLimitExceeded() {
            long byteCnt = getByteCount();
            long byteThreshold = getByteThreshold();
            if (byteThreshold < byteCnt) {
                return "\n\tAttribute: '" + attributeName + "' Value '" + attributeValue + "'"
                        + "\tByte threshold: " + byteThreshold
                        + "\tExpected Count: " + byteCnt + " (counting this file)"
                        + ((byteThreshold < byteCnt) ? "\t[threshold exceeded]" : "\t[below thresholds]");
            }
            return null;
        }

        public boolean fileThresholdExceeded() {
            return getFileThreshold() < getFileCount();
        }

        public boolean byteThresholdExceeded() {
            return getByteThreshold() < getByteCount();
        }

        public long getByteCount() {
            compareRecordReadLock.lock();
            try {
                return byteCount;
            } finally {
                compareRecordReadLock.unlock();
            }
        }

        public int getFileCount() {
            compareRecordReadLock.lock();
            try {
                return fileCount;
            } finally {
                compareRecordReadLock.unlock();
            }
        }

        public long getByteThreshold() {
            compareRecordReadLock.lock();
            try {
                return threshold.getByteCount();
            } finally {
                compareRecordReadLock.unlock();
            }
        }

        public int getFileThreshold() {
            compareRecordReadLock.lock();
            try {
                return threshold.getFileCount();
            } finally {
                compareRecordReadLock.unlock();
            }
        }

        public String getAttributeName() {
            return attributeName;
        }

        public String getAttributeValue() {
            return this.attributeValue;
        }

    } //end CompareRecord inner class

} // end MonitorThreshold class
