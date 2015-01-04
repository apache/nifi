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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.annotation.TriggerWhenEmpty;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.Bin;
import org.apache.nifi.processors.standard.util.BinManager;
import org.apache.nifi.processors.standard.util.FlowFileSessionWrapper;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV2;
import org.apache.nifi.util.FlowFilePackagerV3;
import org.apache.nifi.util.ObjectHolder;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

@SideEffectFree
@TriggerWhenEmpty
@Tags({"merge", "content", "correlation", "tar", "zip", "stream", "concatenation", "archive", "flowfile-stream", "flowfile-stream-v3"})
@CapabilityDescription("Merges a Group of FlowFiles together based on a user-defined strategy and packages them into a single FlowFile. It is recommended that the Processor be configured with only a single incoming connection, as Group of FlowFiles will not be created from FlowFiles in different connections. This processor updates the mime.type attribute as appropriate.")
public class MergeContent extends AbstractSessionFactoryProcessor {

    // preferred attributes
    public static final String FRAGMENT_ID_ATTRIBUTE = "fragment.identifier";
    public static final String FRAGMENT_INDEX_ATTRIBUTE = "fragment.index";
    public static final String FRAGMENT_COUNT_ATTRIBUTE = "fragment.count";

    // old style attributes
    public static final String SEGMENT_ID_ATTRIBUTE = "segment.identifier";
    public static final String SEGMENT_INDEX_ATTRIBUTE = "segment.index";
    public static final String SEGMENT_COUNT_ATTRIBUTE = "segment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = "segment.original.filename";

    public static final AllowableValue MERGE_STRATEGY_BIN_PACK = new AllowableValue(
            "Bin-Packing Algorithm",
            "Bin-Packing Algorithm",
            "Generates 'bins' of FlowFiles and fills each bin as full as possible. FlowFiles are placed into a bin based on their size and optionally their attributes (if the <Correlation Attribute> property is set)");
    public static final AllowableValue MERGE_STRATEGY_DEFRAGMENT = new AllowableValue(
            "Defragment",
            "Defragment",
            "Combines fragments that are associated by attributes back into a single cohesive FlowFile. If using this strategy, all FlowFiles must have the attributes <fragment.identifier>, <fragment.count>, and <fragment.index> or alternatively (for backward compatibility purposes) <segment.identifier>, <segment.count>, and <segment.index>");

    public static final String MERGE_FORMAT_TAR_VALUE = "TAR";
    public static final String MERGE_FORMAT_ZIP_VALUE = "ZIP";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE = "FlowFile Stream, v3";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE = "FlowFile Stream, v2";
    public static final String MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE = "FlowFile Tar, v1";
    public static final String MERGE_FORMAT_CONCAT_VALUE = "Binary Concatenation";

    public static final AllowableValue MERGE_FORMAT_TAR = new AllowableValue(
            MERGE_FORMAT_TAR_VALUE,
            MERGE_FORMAT_TAR_VALUE,
            "A bin of FlowFiles will be combined into a single TAR file. The FlowFiles' <path> attribute will be used to create a directory in the TAR file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the TAR file. If a FlowFile has an attribute named <tar.permissions> that is 3 characters, each between 0-7, that attribute will be used as the TAR entry's 'mode'.");
    public static final AllowableValue MERGE_FORMAT_ZIP = new AllowableValue(
            MERGE_FORMAT_ZIP_VALUE,
            MERGE_FORMAT_ZIP_VALUE,
            "A bin of FlowFiles will be combined into a single ZIP file. The FlowFiles' <path> attribute will be used to create a directory in the ZIP file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the ZIP file. The <Compression Level> property indicates the ZIP compression to use.");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V3 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
            MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
            "A bin of FlowFiles will be combined into a single Version 3 FlowFile Stream");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V2 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
            MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
            "A bin of FlowFiles will be combined into a single Version 2 FlowFile Stream");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_TAR_V1 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
            MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
            "A bin of FlowFiles will be combined into a single Version 1 FlowFile Package");
    public static final AllowableValue MERGE_FORMAT_CONCAT = new AllowableValue(
            MERGE_FORMAT_CONCAT_VALUE,
            MERGE_FORMAT_CONCAT_VALUE,
            "The contents of all FlowFiles will be concatenated together into a single FlowFile");

    public static final String ATTRIBUTE_STRATEGY_ALL_COMMON = "Keep Only Common Attributes";
    public static final String ATTRIBUTE_STRATEGY_ALL_UNIQUE = "Keep All Unique Attributes";

    public static final String TAR_PERMISSIONS_ATTRIBUTE = "tar.permissions";
    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";

    public static final PropertyDescriptor MERGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Merge Strategy")
            .description("Specifies the algorithm used to merge content. The 'Defragment' algorithm combines fragments that are associated by attributes back into a single cohesive FlowFile. The 'Bin-Packing Algorithm' generates a FlowFile populated by arbitrarily chosen FlowFiles")
            .required(true)
            .allowableValues(MERGE_STRATEGY_BIN_PACK, MERGE_STRATEGY_DEFRAGMENT)
            .defaultValue(MERGE_STRATEGY_BIN_PACK.getValue())
            .build();
    public static final PropertyDescriptor MERGE_FORMAT = new PropertyDescriptor.Builder()
            .required(true)
            .name("Merge Format")
            .description("Determines the format that will be used to merge the content.")
            .allowableValues(MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP, MERGE_FORMAT_FLOWFILE_STREAM_V3, MERGE_FORMAT_FLOWFILE_STREAM_V2, MERGE_FORMAT_FLOWFILE_TAR_V1, MERGE_FORMAT_CONCAT)
            .defaultValue(MERGE_FORMAT_CONCAT.getValue())
            .build();
    public static final PropertyDescriptor ATTRIBUTE_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Attribute Strategy")
            .description("Determines which FlowFile attributes should be added to the bundle. If 'Keep All Unique Attributes' is selected, any attribute on any FlowFile that gets bundled will be kept unless its value conflicts with the value from another FlowFile. If 'Keep Only Common Attributes' is selected, only the attributes that exist on all FlowFiles in the bundle, with the same value, will be preserved.")
            .allowableValues(ATTRIBUTE_STRATEGY_ALL_COMMON, ATTRIBUTE_STRATEGY_ALL_UNIQUE)
            .defaultValue(ATTRIBUTE_STRATEGY_ALL_COMMON)
            .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Correlation Attribute Name")
            .description("If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .defaultValue(null)
            .build();

    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("Header File")
            .description("Filename specifying the header to use. If not specified, no header is supplied. This property is valid only when using the binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor FOOTER = new PropertyDescriptor.Builder()
            .name("Footer File")
            .description("Filename specifying the footer to use. If not specified, no footer is supplied. This property is valid only when using the binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Demarcator File")
            .description("Filename specifying the demarcator to use. If not specified, no demarcator is supplied. This property is valid only when using the binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("Compression Level")
            .description("Specifies the compression level to use when using the Zip Merge Format; if not using the Zip Merge Format, this value is ignored")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor KEEP_PATH = new PropertyDescriptor.Builder()
            .name("Keep Path")
            .description("If using the Zip or Tar Merge Format, specifies whether or not the FlowFiles' paths should be included in their entry names; if using other merge strategy, this value is ignored")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum Group Size")
            .description("The minimum size of for the bundle")
            .required(true)
            .defaultValue("0 B")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Group Size")
            .description("The maximum size for the bundle. If not specified, there is no maximum.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_ENTRIES = new PropertyDescriptor.Builder()
            .name("Minimum Number of Entries")
            .description("The minimum number of files to include in a bundle")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor MAX_ENTRIES = new PropertyDescriptor.Builder()
            .name("Maximum Number of Entries")
            .description("The maximum number of files to include in a bundle. If not specified, there is no maximum.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BIN_COUNT = new PropertyDescriptor.Builder()
            .name("Maximum number of Bins")
            .description("Specifies the maximum number of bins that can be held in memory at any one time")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BIN_AGE = new PropertyDescriptor.Builder()
            .name("Max Bin Age")
            .description("The maximum age of a Bin that will trigger a Bin to be complete. Expected format is <duration> <time unit> where <duration> is a positive integer and time unit is one of seconds, minutes, hours")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").description("The FlowFiles that were used to create the bundle").build();
    public static final Relationship REL_MERGED = new Relationship.Builder().name("merged").description("The FlowFile containing the merged content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("If the bundle cannot be created, all FlowFiles that would have been used to created the bundle will be transferred to failure").build();

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private final BinManager binManager = new BinManager();

    private final Queue<Bin> readyBins = new LinkedBlockingQueue<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_MERGED);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MERGE_STRATEGY);
        descriptors.add(MERGE_FORMAT);
        descriptors.add(ATTRIBUTE_STRATEGY);
        descriptors.add(CORRELATION_ATTRIBUTE_NAME);
        descriptors.add(MIN_ENTRIES);
        descriptors.add(MAX_ENTRIES);
        descriptors.add(MIN_SIZE);
        descriptors.add(MAX_SIZE);
        descriptors.add(MAX_BIN_AGE);
        descriptors.add(MAX_BIN_COUNT);
        descriptors.add(HEADER);
        descriptors.add(FOOTER);
        descriptors.add(DEMARCATOR);
        descriptors.add(COMPRESSION_LEVEL);
        descriptors.add(KEEP_PATH);

        this.descriptors = Collections.unmodifiableList(descriptors);
    }

    @OnStopped
    public void resetState() {
        binManager.purge();

        Bin bin;
        while ((bin = readyBins.poll()) != null) {
            for (final FlowFileSessionWrapper wrapper : bin.getContents()) {
                wrapper.getSession().rollback();
            }
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private byte[] readContent(final String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        int binsAdded = binFlowFiles(context, sessionFactory);
        getLogger().debug("Binned {} FlowFiles", new Object[] {binsAdded});
        
        if (!isScheduled()) {
            return;
        }

        binsAdded += migrateBins(context);

        final int binsProcessed = processBins(context, sessionFactory);
        if (binsProcessed == 0 && binsAdded == 0) {
            context.yield();
        }
    }
    

    private int migrateBins(final ProcessContext context) {
        int added = 0;
        for (final Bin bin : binManager.removeReadyBins(true)) {
            this.readyBins.add(bin);
            added++;
        }

        // if we have created all of the bins that are allowed, go ahead and remove the oldest one. If we don't do
        // this, then we will simply wait for it to expire because we can't get any more FlowFiles into the
        // bins. So we may as well expire it now.
        if (added == 0 && binManager.getBinCount() >= context.getProperty(MAX_BIN_COUNT).asInteger()) {
            final Bin bin = binManager.removeOldestBin();
            if (bin != null) {
                added++;
                this.readyBins.add(bin);
            }
        }

        return added;
    }

    private int processBins(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        final Bin bin = readyBins.poll();
        if (bin == null) {
            return 0;
        }

        final String mergeFormat = context.getProperty(MERGE_FORMAT).getValue();
        MergeBin merger;
        switch (mergeFormat) {
            case MERGE_FORMAT_TAR_VALUE:
                merger = new TarMerge();
                break;
            case MERGE_FORMAT_ZIP_VALUE:
                merger = new ZipMerge(context.getProperty(COMPRESSION_LEVEL).asInteger());
                break;
            case MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV3(), "application/flowfile-v3");
                break;
            case MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV2(), "application/flowfile-v2");
                break;
            case MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV1(), "application/flowfile-v1");
                break;
            case MERGE_FORMAT_CONCAT_VALUE:
                merger = new BinaryConcatenationMerge();
                break;
            default:
                throw new AssertionError();
        }

        final AttributeStrategy attributeStrategy;
        switch (context.getProperty(ATTRIBUTE_STRATEGY).getValue()) {
            case ATTRIBUTE_STRATEGY_ALL_UNIQUE:
                attributeStrategy = new KeepUniqueAttributeStrategy();
                break;
            case ATTRIBUTE_STRATEGY_ALL_COMMON:
            default:
                attributeStrategy = new KeepCommonAttributeStrategy();
                break;
        }

        final List<Bin> bins = new ArrayList<>();
        bins.add(bin);

        final ProcessorLog logger = getLogger();
        final ProcessSession session = sessionFactory.createSession();

        final Set<Bin> committedBins = new HashSet<>();
        
        for (final Bin unmodifiableBin : bins) {
            final List<FlowFileSessionWrapper> binCopy = new ArrayList<>(unmodifiableBin.getContents());

            if (MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
                final String error = getDefragmentValidationError(binCopy);
                if (error != null) {
                    final String binDescription = binCopy.size() <= 10 ? binCopy.toString() : binCopy.size() + " FlowFiles";
                    logger.error(error + "; routing {} to failure", new Object[]{binDescription});
                    for ( final FlowFileSessionWrapper wrapper : binCopy ) {
                        wrapper.getSession().transfer(wrapper.getFlowFile(), REL_FAILURE);
                        wrapper.getSession().commit();
                        committedBins.add(unmodifiableBin);
                    }
                    
                    continue;
                }

                Collections.sort(binCopy, new FragmentComparator());
            }

            FlowFile bundle = null;
            try {
                bundle = merger.merge(context, session, binCopy);

                // keep the filename, as it is added to the bundle.
                final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

                // merge all of the attributes
                final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(binCopy);
                bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
                // restore the filename of the bundle
                bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
                bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(binCopy.size()));
                bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));

                bundle = session.putAllAttributes(bundle, bundleAttributes);

                final String inputDescription = (binCopy.size() < 10) ? binCopy.toString() : binCopy.size() + " FlowFiles";
                logger.info("Merged {} into {}", new Object[]{inputDescription, bundle});
            } catch (final Exception e) {
                logger.error("Failed to process bundle of {} files due to {}", new Object[]{binCopy.size(), e});

                for (final FlowFileSessionWrapper wrapper : binCopy) {
                    wrapper.getSession().transfer(wrapper.getFlowFile(), REL_FAILURE);
                    wrapper.getSession().commit();
                }
                session.rollback();
                return 1;
            }
            session.transfer(bundle, REL_MERGED);
        }

        // we first commit the bundle's session before the originals' sessions because if we are restarted or crash
        // between commits, we favor data redundancy over data loss. Since we have no Distributed Transaction capability
        // across multiple sessions, we cannot guarantee atomicity across the sessions
        session.commit();
        for (final Bin unmodifiableBin : bins) {
            // If this bin's session has been committed, move on.
            if ( committedBins.contains(unmodifiableBin) ) {
                continue;
            }
            
            for (final FlowFileSessionWrapper wrapper : unmodifiableBin.getContents()) {
                wrapper.getSession().transfer(wrapper.getFlowFile(), REL_ORIGINAL);
                wrapper.getSession().commit();
            }
        }

        return 1;
    }

    private int binFlowFiles(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        int binsAdded = 0;
        while (binManager.getBinCount() < context.getProperty(MAX_BIN_COUNT).asInteger().intValue()) {
            if (!isScheduled()) {
                return binsAdded;
            }

            final ProcessSession session = sessionFactory.createSession();
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                return binsAdded;
            }

            // handle backward compatibility with old segment attributes
            if (flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE) == null && flowFile.getAttribute(SEGMENT_COUNT_ATTRIBUTE) != null) {
                flowFile = session.putAttribute(flowFile, FRAGMENT_COUNT_ATTRIBUTE, flowFile.getAttribute(SEGMENT_COUNT_ATTRIBUTE));
            }
            if (flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE) == null && flowFile.getAttribute(SEGMENT_INDEX_ATTRIBUTE) != null) {
                flowFile = session.putAttribute(flowFile, FRAGMENT_INDEX_ATTRIBUTE, flowFile.getAttribute(SEGMENT_INDEX_ATTRIBUTE));
            }
            if (flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE) == null && flowFile.getAttribute(SEGMENT_ID_ATTRIBUTE) != null) {
                flowFile = session.putAttribute(flowFile, FRAGMENT_ID_ATTRIBUTE, flowFile.getAttribute(SEGMENT_ID_ATTRIBUTE));
            }

            final String correlationAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME).getValue();
            String groupId = (correlationAttributeName == null) ? null : flowFile.getAttribute(correlationAttributeName);

            // when MERGE_STRATEGY is Defragment and correlationAttributeName is null then bin by fragment.identifier
            if (groupId == null && MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
                groupId = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
            }

            final boolean binned = binManager.offer(groupId, flowFile, session);

            // could not be added to a bin -- probably too large by itself, so create a separate bin for just this guy.
            if (!binned) {
                Bin bin = new Bin(0, Long.MAX_VALUE, 0, Integer.MAX_VALUE, null);
                bin.offer(flowFile, session);
                this.readyBins.add(bin);
            }

            binsAdded++;
        }

        return binsAdded;
    }

    private String getDefragmentValidationError(final List<FlowFileSessionWrapper> bin) {
        if (bin.isEmpty()) {
            return null;
        }

        // If we are defragmenting, all fragments must have the appropriate attributes.
        String decidedFragmentCount = null;
        String fragmentIdentifier = null;
        for (final FlowFileSessionWrapper flowFileWrapper : bin) {
            final FlowFile flowFile = flowFileWrapper.getFlowFile();

            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE);
            if (!isNumber(fragmentIndex)) {
                return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_INDEX_ATTRIBUTE + " attribute";
            }
            
            fragmentIdentifier = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);

            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE);
            if (!isNumber(fragmentCount)) {
                return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute";
            } else if (decidedFragmentCount == null) {
                decidedFragmentCount = fragmentCount;
            } else if (!decidedFragmentCount.equals(fragmentCount)) {
                return "Cannot Defragment " + flowFile + " because it is grouped with another FlowFile, and the two have differing values for the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute: " + decidedFragmentCount + " and " + fragmentCount;
            }
        }
        
        final int numericFragmentCount;
        try {
            numericFragmentCount = Integer.parseInt(decidedFragmentCount);
        } catch (final NumberFormatException nfe) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the " + FRAGMENT_COUNT_ATTRIBUTE + " has a non-integer value of " + decidedFragmentCount;
        }
        
        if ( bin.size() < numericFragmentCount ) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found only " + bin.size() + " fragments";
        }
        
        if ( bin.size() > numericFragmentCount ) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found " + bin.size() + " fragments for this identifier";
        }

        return null;
    }

    private boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        return NUMBER_PATTERN.matcher(value).matches();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        binManager.setMinimumSize(context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue());

        if (context.getProperty(MAX_BIN_AGE).isSet() ) {
            binManager.setMaxBinAge(context.getProperty(MAX_BIN_AGE).asTimePeriod(TimeUnit.SECONDS).intValue());
        } else {
            binManager.setMaxBinAge(Integer.MAX_VALUE);
        }
        
        if ( context.getProperty(MAX_SIZE).isSet() ) {
            binManager.setMaximumSize(context.getProperty(MAX_SIZE).asDataSize(DataUnit.B).longValue());
        } else {
            binManager.setMaximumSize(Long.MAX_VALUE);
        }
        
        if (MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            binManager.setFileCountAttribute(FRAGMENT_COUNT_ATTRIBUTE);
        } else {
            binManager.setMinimumEntries(context.getProperty(MIN_ENTRIES).asInteger());

            if ( context.getProperty(MAX_ENTRIES).isSet() ) {
                binManager.setMaximumEntries(context.getProperty(MAX_ENTRIES).asInteger().intValue());
            } else {
                binManager.setMaximumEntries(Integer.MAX_VALUE);
            }
        }

    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final long minBytes = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxBytes = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);

        if (maxBytes != null && maxBytes.longValue() < minBytes) {
            problems.add(new ValidationResult.Builder().subject(MIN_SIZE.getName()).input(
                    context.getProperty(MIN_SIZE).getValue()).valid(false).explanation("Min Size must be less than or equal to Max Size").build());
        }

        final Long min = context.getProperty(MIN_ENTRIES).asLong();
        final Long max = context.getProperty(MAX_ENTRIES).asLong();

        if (min != null && max != null) {
            if (min > max) {
                problems.add(new ValidationResult.Builder().subject(MIN_ENTRIES.getName()).input(context.getProperty(MIN_ENTRIES).getValue()).valid(false).explanation("Min Entries must be less than or equal to Max Entries").build());
            }
        }

        return problems;
    }

    private class BinaryConcatenationMerge implements MergeBin {

        private String mimeType = "application/octet-stream";

        public BinaryConcatenationMerge() {
        }

        @Override
        public FlowFile merge(final ProcessContext context, final ProcessSession session, final List<FlowFileSessionWrapper> wrappers) {
            final Set<FlowFile> parentFlowFiles = new HashSet<>();
            for (final FlowFileSessionWrapper wrapper : wrappers) {
                parentFlowFiles.add(wrapper.getFlowFile());
            }

            FlowFile bundle = session.create(parentFlowFiles);
            final ObjectHolder<String> bundleMimeTypeRef = new ObjectHolder<>(null);
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    final byte[] header = getDescriptorFileContent(context, wrappers, HEADER);
                    if (header != null) {
                        out.write(header);
                    }

                    boolean isFirst = true;
                    final Iterator<FlowFileSessionWrapper> itr = wrappers.iterator();
                    while (itr.hasNext()) {
                        final FlowFileSessionWrapper wrapper = itr.next();
                        wrapper.getSession().read(wrapper.getFlowFile(), new InputStreamCallback() {
                            @Override
                            public void process(final InputStream in) throws IOException {
                                StreamUtils.copy(in, out);
                            }
                        });

                        if (itr.hasNext()) {
                            final byte[] demarcator = getDescriptorFileContent(context, wrappers, DEMARCATOR);
                            if (demarcator != null) {
                                out.write(demarcator);
                            }
                        }

                        final String flowFileMimeType = wrapper.getFlowFile().getAttribute(CoreAttributes.MIME_TYPE.key());
                        if (isFirst) {
                            bundleMimeTypeRef.set(flowFileMimeType);
                            isFirst = false;
                        } else {
                            if (bundleMimeTypeRef.get() != null && !bundleMimeTypeRef.get().equals(flowFileMimeType)) {
                                bundleMimeTypeRef.set(null);
                            }
                        }
                    }

                    final byte[] footer = getDescriptorFileContent(context, wrappers, FOOTER);
                    if (footer != null) {
                        out.write(footer);
                    }
                }
            });

            session.getProvenanceReporter().join(getFlowFiles(wrappers), bundle);
            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(wrappers));
            if (bundleMimeTypeRef.get() != null) {
                this.mimeType = bundleMimeTypeRef.get();
            }

            return bundle;
        }

        private byte[] getDescriptorFileContent(final ProcessContext context, final List<FlowFileSessionWrapper> wrappers, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            final String descriptorFile = context.getProperty(descriptor).getValue();
            if (descriptorFile != null && wrappers != null && wrappers.size() > 0) {
                final String content = new String(readContent(descriptorFile));
                final FlowFileSessionWrapper wrapper = wrappers.get(0);
                if (wrapper != null && content != null) {
                    final FlowFile flowFile = wrapper.getFlowFile();
                    if (flowFile != null) {
                        final PropertyValue propVal = context.newPropertyValue(content).evaluateAttributeExpressions(flowFile);
                        property = propVal.getValue().getBytes();
                    }
                }
            }
            return property;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }
    }

    private List<FlowFile> getFlowFiles(final List<FlowFileSessionWrapper> sessionWrappers) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final FlowFileSessionWrapper wrapper : sessionWrappers) {
            flowFiles.add(wrapper.getFlowFile());
        }
        return flowFiles;
    }

    private String getPath(final FlowFile flowFile) {
        Path path = Paths.get(flowFile.getAttribute(CoreAttributes.PATH.key()));
        if (path.getNameCount() == 0) {
            return "";
        }

        if (".".equals(path.getName(0).toString())) {
            path = (path.getNameCount() == 1) ? null : path.subpath(1, path.getNameCount());
        }

        return (path == null) ? "" : path.toString() + "/";
    }

    private String createFilename(final List<FlowFileSessionWrapper> wrappers) {
        if (wrappers.size() == 1) {
            return wrappers.get(0).getFlowFile().getAttribute(CoreAttributes.FILENAME.key());
        } else {
            FlowFile ff = wrappers.get(0).getFlowFile();
            String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
            if (origFilename != null) {
                return origFilename;
            } else {
                return String.valueOf(System.nanoTime());
            }
        }
    }

    private class TarMerge implements MergeBin {

        @Override
        public FlowFile merge(final ProcessContext context, final ProcessSession session, final List<FlowFileSessionWrapper> wrappers) {
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();
            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(wrappers) + ".tar");
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final TarArchiveOutputStream out = new TarArchiveOutputStream(bufferedOut)) {
                        out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                        for (final FlowFileSessionWrapper wrapper : wrappers) {
                            final FlowFile flowFile = wrapper.getFlowFile();

                            final String path = keepPath ? getPath(flowFile) : "";
                            final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());

                            final TarArchiveEntry tarEntry = new TarArchiveEntry(entryName);
                            tarEntry.setSize(flowFile.getSize());
                            final String permissionsVal = flowFile.getAttribute(TAR_PERMISSIONS_ATTRIBUTE);
                            if (permissionsVal != null) {
                                try {
                                    tarEntry.setMode(Integer.parseInt(permissionsVal));
                                } catch (final Exception e) {
                                    getLogger().debug("Attribute {} of {} is set to {}; expected 3 digits between 0-7, so ignoring",
                                            new Object[]{TAR_PERMISSIONS_ATTRIBUTE, flowFile, permissionsVal});
                                }
                            }

                            out.putArchiveEntry(tarEntry);

                            wrapper.getSession().exportTo(flowFile, out);
                            out.closeArchiveEntry();
                        }
                    }
                }
            });

            session.getProvenanceReporter().join(getFlowFiles(wrappers), bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/tar";
        }
    }

    private class FlowFileStreamMerger implements MergeBin {

        private final FlowFilePackager packager;
        private final String mimeType;

        public FlowFileStreamMerger(final FlowFilePackager packager, final String mimeType) {
            this.packager = packager;
            this.mimeType = mimeType;
        }

        @Override
        public FlowFile merge(final ProcessContext context, final ProcessSession session, final List<FlowFileSessionWrapper> wrappers) {
            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut)) {
                        // we don't want the packager closing the stream. V1 creates a TAR Output Stream, which then gets
                        // closed, which in turn closes the underlying OutputStream, and we want to protect ourselves against that.
                        final OutputStream out = new NonCloseableOutputStream(bufferedOut);

                        for (final FlowFileSessionWrapper wrapper : wrappers) {
                            final FlowFile flowFile = wrapper.getFlowFile();
                            wrapper.getSession().read(flowFile, new InputStreamCallback() {
                                @Override
                                public void process(final InputStream rawIn) throws IOException {
                                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                                        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

                                        // for backward compatibility purposes, we add the "legacy" NiFi attributes
                                        attributes.put("nf.file.name", attributes.get(CoreAttributes.FILENAME.key()));
                                        attributes.put("nf.file.path", attributes.get(CoreAttributes.PATH.key()));
                                        if (attributes.containsKey(CoreAttributes.MIME_TYPE.key())) {
                                            attributes.put("content-type", attributes.get(CoreAttributes.MIME_TYPE.key()));
                                        }

                                        packager.packageFlowFile(in, out, attributes, flowFile.getSize());
                                    }
                                }
                            });
                        }
                    }
                }
            });

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(wrappers) + ".pkg");
            session.getProvenanceReporter().join(getFlowFiles(wrappers), bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }
    }

    private class ZipMerge implements MergeBin {

        private final int compressionLevel;

        public ZipMerge(final int compressionLevel) {
            this.compressionLevel = compressionLevel;
        }

        @Override
        public FlowFile merge(final ProcessContext context, final ProcessSession session, final List<FlowFileSessionWrapper> wrappers) {
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();

            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(wrappers) + ".zip");
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final ZipOutputStream out = new ZipOutputStream(bufferedOut)) {
                        out.setLevel(compressionLevel);
                        for (final FlowFileSessionWrapper wrapper : wrappers) {
                            final FlowFile flowFile = wrapper.getFlowFile();

                            final String path = keepPath ? getPath(flowFile) : "";
                            final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());
                            final ZipEntry zipEntry = new ZipEntry(entryName);
                            zipEntry.setSize(flowFile.getSize());
                            out.putNextEntry(zipEntry);

                            wrapper.getSession().exportTo(flowFile, out);
                            out.closeEntry();
                        }

                        out.finish();
                        out.flush();
                    }
                }
            });

            session.getProvenanceReporter().join(getFlowFiles(wrappers), bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/zip";
        }
    }

    private static class KeepUniqueAttributeStrategy implements AttributeStrategy {

        @Override
        public Map<String, String> getMergedAttributes(final List<FlowFileSessionWrapper> flowFiles) {
            final Map<String, String> newAttributes = new HashMap<>();
            final Set<String> conflicting = new HashSet<>();

            for (final FlowFileSessionWrapper wrapper : flowFiles) {
                final FlowFile flowFile = wrapper.getFlowFile();

                for (final Map.Entry<String, String> attributeEntry : flowFile.getAttributes().entrySet()) {
                    final String name = attributeEntry.getKey();
                    final String value = attributeEntry.getValue();

                    final String existingValue = newAttributes.get(name);
                    if (existingValue != null && !existingValue.equals(value)) {
                        conflicting.add(name);
                    } else {
                        newAttributes.put(name, value);
                    }
                }
            }

            for (final String attributeToRemove : conflicting) {
                newAttributes.remove(attributeToRemove);
            }

            // Never copy the UUID from the parents - which could happen if we don't remove it and there is only 1 parent.
            newAttributes.remove(CoreAttributes.UUID.key());
            return newAttributes;
        }
    }

    private static class KeepCommonAttributeStrategy implements AttributeStrategy {

        @Override
        public Map<String, String> getMergedAttributes(final List<FlowFileSessionWrapper> flowFiles) {
            final Map<String, String> result = new HashMap<>();

            //trivial cases
            if (flowFiles == null || flowFiles.isEmpty()) {
                return result;
            } else if (flowFiles.size() == 1) {
                result.putAll(flowFiles.iterator().next().getFlowFile().getAttributes());
            }

            /*
             * Start with the first attribute map and only put an entry to the
             * resultant map if it is common to every map.
             */
            final Map<String, String> firstMap = flowFiles.iterator().next().getFlowFile().getAttributes();

            outer:
            for (final Map.Entry<String, String> mapEntry : firstMap.entrySet()) {
                final String key = mapEntry.getKey();
                final String value = mapEntry.getValue();

                for (final FlowFileSessionWrapper flowFileWrapper : flowFiles) {
                    final Map<String, String> currMap = flowFileWrapper.getFlowFile().getAttributes();
                    final String curVal = currMap.get(key);
                    if (curVal == null || !curVal.equals(value)) {
                        continue outer;
                    }
                }
                result.put(key, value);
            }

            // Never copy the UUID from the parents - which could happen if we don't remove it and there is only 1 parent.
            result.remove(CoreAttributes.UUID.key());
            return result;
        }
    }

    private static class FragmentComparator implements Comparator<FlowFileSessionWrapper> {

        @Override
        public int compare(final FlowFileSessionWrapper o1, final FlowFileSessionWrapper o2) {
            final int fragmentIndex1 = Integer.parseInt(o1.getFlowFile().getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            final int fragmentIndex2 = Integer.parseInt(o2.getFlowFile().getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            return Integer.compare(fragmentIndex1, fragmentIndex2);
        }
    }

    private interface MergeBin {

        FlowFile merge(ProcessContext context, ProcessSession session, List<FlowFileSessionWrapper> flowFiles);

        String getMergedContentType();
    }

    private interface AttributeStrategy {

        Map<String, String> getMergedAttributes(List<FlowFileSessionWrapper> flowFiles);
    }
}
