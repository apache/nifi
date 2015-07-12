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
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.Bin;
import org.apache.nifi.processors.standard.util.BinManager;
import org.apache.nifi.processors.standard.util.FlowFileSessionWrapper;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV2;
import org.apache.nifi.util.FlowFilePackagerV3;
import org.apache.nifi.util.ObjectHolder;

@SideEffectFree
@TriggerWhenEmpty
@Tags({"merge", "content", "correlation", "tar", "zip", "stream", "concatenation", "archive", "flowfile-stream", "flowfile-stream-v3"})
@CapabilityDescription("Merges a Group of FlowFiles together based on a user-defined strategy and packages them into a single FlowFile. "
        + "It is recommended that the Processor be configured with only a single incoming connection, as Group of FlowFiles will not be "
        + "created from FlowFiles in different connections. This processor updates the mime.type attribute as appropriate.")
@ReadsAttributes({
    @ReadsAttribute(attribute = "fragment.identifier", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "All FlowFiles with the same value for this attribute will be bundled together."),
    @ReadsAttribute(attribute = "fragment.index", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "This attribute indicates the order in which the fragments should be assembled. This "
        + "attribute must be present on all FlowFiles when using the Defragment Merge Strategy and must be a unique (i.e., unique across all "
        + "FlowFiles that have the same value for the \"fragment.identifier\" attribute) integer "
        + "between 0 and the value of the fragment.count attribute. If two or more FlowFiles have the same value for the "
        + "\"fragment.identifier\" attribute and the same value for the \"fragment.index\" attribute, the behavior of this Processor is undefined."),
    @ReadsAttribute(attribute = "fragment.count", description = "Applicable only if the <Merge Strategy> property is set to Defragment. This "
        + "attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
        + "bundle must have the same value for this attribute. The value of this attribute indicates how many FlowFiles should be expected "
        + "in the given bundle."),
    @ReadsAttribute(attribute = "segment.original.filename", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "This attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
        + "bundle must have the same value for this attribute. The value of this attribute will be used for the filename of the completed merged "
        + "FlowFile."),
    @ReadsAttribute(attribute = "tar.permissions", description = "Applicable only if the <Merge Format> property is set to TAR. The value of this "
        + "attribute must be 3 characters; each character must be in the range 0 to 7 (inclusive) and indicates the file permissions that should "
        + "be used for the FlowFile's TAR entry. If this attribute is missing or has an invalid value, the default value of 644 will be used") })
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "When more than 1 file is merged, the filename comes from the segment.original.filename "
        + "attribute. If that attribute does not exist in the source FlowFiles, then the filename is set to the number of nanoseconds matching "
        + "system time. Then a filename extension may be applied:"
        + "if Merge Format is TAR, then the filename will be appended with .tar, "
        + "if Merge Format is ZIP, then the filename will be appended with .zip, "
        + "if Merge Format is FlowFileStream, then the filename will be appended with .pkg"),
    @WritesAttribute(attribute = "merge.count", description = "The number of FlowFiles that were merged into this bundle"),
    @WritesAttribute(attribute = "merge.bin.age", description = "The age of the bin, in milliseconds, when it was merged and output. Effectively "
        + "this is the greatest amount of time that any FlowFile in this bundle remained waiting in this processor before it was output") })
@SeeAlso(SegmentContent.class)
public class MergeContent extends BinFiles {

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
            "Generates 'bins' of FlowFiles and fills each bin as full as possible. FlowFiles are placed into a bin based on their size and optionally "
            + "their attributes (if the <Correlation Attribute> property is set)");
    public static final AllowableValue MERGE_STRATEGY_DEFRAGMENT = new AllowableValue(
            "Defragment",
            "Defragment",
            "Combines fragments that are associated by attributes back into a single cohesive FlowFile. If using this strategy, all FlowFiles must "
            + "have the attributes <fragment.identifier>, <fragment.count>, and <fragment.index> or alternatively (for backward compatibility "
            + "purposes) <segment.identifier>, <segment.count>, and <segment.index>. All FlowFiles with the same value for \"fragment.identifier\" "
            + "will be grouped together. All FlowFiles in this group must have the same value for the \"fragment.count\" attribute. All FlowFiles "
            + "in this group must have a unique value for the \"fragment.index\" attribute between 0 and the value of the \"fragment.count\" attribute.");

    public static final AllowableValue DELIMITER_STRATEGY_FILENAME = new AllowableValue(
            "Filename", "Filename", "The values of Header, Footer, and Demarcator will be retrieved from the contents of a file");
    public static final AllowableValue DELIMITER_STRATEGY_TEXT = new AllowableValue(
            "Text", "Text", "The values of Header, Footer, and Demarcator will be specified as property values");

    public static final String MERGE_FORMAT_TAR_VALUE = "TAR";
    public static final String MERGE_FORMAT_ZIP_VALUE = "ZIP";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE = "FlowFile Stream, v3";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE = "FlowFile Stream, v2";
    public static final String MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE = "FlowFile Tar, v1";
    public static final String MERGE_FORMAT_CONCAT_VALUE = "Binary Concatenation";

    public static final AllowableValue MERGE_FORMAT_TAR = new AllowableValue(
            MERGE_FORMAT_TAR_VALUE,
            MERGE_FORMAT_TAR_VALUE,
            "A bin of FlowFiles will be combined into a single TAR file. The FlowFiles' <path> attribute will be used to create a directory in the "
            + "TAR file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the TAR file. "
            + "If a FlowFile has an attribute named <tar.permissions> that is 3 characters, each between 0-7, that attribute will be used "
            + "as the TAR entry's 'mode'.");
    public static final AllowableValue MERGE_FORMAT_ZIP = new AllowableValue(
            MERGE_FORMAT_ZIP_VALUE,
            MERGE_FORMAT_ZIP_VALUE,
            "A bin of FlowFiles will be combined into a single ZIP file. The FlowFiles' <path> attribute will be used to create a directory in the "
            + "ZIP file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the ZIP file. "
            + "The <Compression Level> property indicates the ZIP compression to use.");
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
            .description("Specifies the algorithm used to merge content. The 'Defragment' algorithm combines fragments that are associated by "
                    + "attributes back into a single cohesive FlowFile. The 'Bin-Packing Algorithm' generates a FlowFile populated by arbitrarily "
                    + "chosen FlowFiles")
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
            .description("Determines which FlowFile attributes should be added to the bundle. If 'Keep All Unique Attributes' is selected, any "
                    + "attribute on any FlowFile that gets bundled will be kept unless its value conflicts with the value from another FlowFile. "
                    + "If 'Keep Only Common Attributes' is selected, only the attributes that exist on all FlowFiles in the bundle, with the same "
                    + "value, will be preserved.")
            .allowableValues(ATTRIBUTE_STRATEGY_ALL_COMMON, ATTRIBUTE_STRATEGY_ALL_UNIQUE)
            .defaultValue(ATTRIBUTE_STRATEGY_ALL_COMMON)
            .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Correlation Attribute Name")
            .description("If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for "
                    + "this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .defaultValue(null)
            .build();

    public static final PropertyDescriptor DELIMITER_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Delimiter Strategy")
            .description("Determines if Header, Footer, and Demarcator should point to files containing the respective content, or if "
                    + "the values of the properties should be used as the content.")
            .allowableValues(DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
            .defaultValue(DELIMITER_STRATEGY_FILENAME.getValue())
            .build();
    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("Header File")
            .displayName("Header")
            .description("Filename specifying the header to use. If not specified, no header is supplied. This property is valid only when using the "
                    + "binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor FOOTER = new PropertyDescriptor.Builder()
            .name("Footer File")
            .displayName("Footer")
            .description("Filename specifying the footer to use. If not specified, no footer is supplied. This property is valid only when using the "
                    + "binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Demarcator File")
            .displayName("Demarcator")
            .description("Filename specifying the demarcator to use. If not specified, no demarcator is supplied. This property is valid only when "
                    + "using the binary-concatenation merge strategy; otherwise, it is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("Compression Level")
            .description("Specifies the compression level to use when using the Zip Merge Format; if not using the Zip Merge Format, this value is "
                    + "ignored")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor KEEP_PATH = new PropertyDescriptor.Builder()
            .name("Keep Path")
            .description("If using the Zip or Tar Merge Format, specifies whether or not the FlowFiles' paths should be included in their entry "
                    + "names; if using other merge strategy, this value is ignored")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_MERGED = new Relationship.Builder().name("merged").description("The FlowFile containing the merged content").build();

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_MERGED);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
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
        descriptors.add(DELIMITER_STRATEGY);
        descriptors.add(HEADER);
        descriptors.add(FOOTER);
        descriptors.add(DEMARCATOR);
        descriptors.add(COMPRESSION_LEVEL);
        descriptors.add(KEEP_PATH);
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String delimiterStrategy = context.getProperty(DELIMITER_STRATEGY).getValue();
        if(DELIMITER_STRATEGY_FILENAME.equals(delimiterStrategy)) {
            final String headerValue = context.getProperty(HEADER).getValue();
            if (headerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(HEADER.getName(), headerValue, context));
            }

            final String footerValue = context.getProperty(FOOTER).getValue();
            if (footerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(FOOTER.getName(), footerValue, context));
            }

            final String demarcatorValue = context.getProperty(DEMARCATOR).getValue();
            if (demarcatorValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(DEMARCATOR.getName(), demarcatorValue, context));
            }
        }
        return results;
    }

    private byte[] readContent(final String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    @Override
    protected FlowFile preprocessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        FlowFile processed = flowFile;
        // handle backward compatibility with old segment attributes
        if (processed.getAttribute(FRAGMENT_COUNT_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_COUNT_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_COUNT_ATTRIBUTE, processed.getAttribute(SEGMENT_COUNT_ATTRIBUTE));
        }
        if (processed.getAttribute(FRAGMENT_INDEX_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_INDEX_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_INDEX_ATTRIBUTE, processed.getAttribute(SEGMENT_INDEX_ATTRIBUTE));
        }
        if (processed.getAttribute(FRAGMENT_ID_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_ID_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_ID_ATTRIBUTE, processed.getAttribute(SEGMENT_ID_ATTRIBUTE));
        }

        return processed;
    }

    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile) {
        final String correlationAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME).getValue();
        String groupId = correlationAttributeName == null ? null : flowFile.getAttribute(correlationAttributeName);

        // when MERGE_STRATEGY is Defragment and correlationAttributeName is null then bin by fragment.identifier
        if (groupId == null && MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            groupId = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        return groupId;
    }

    @Override
    protected void setUpBinManager(final BinManager binManager, final ProcessContext context) {
        if (MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            binManager.setFileCountAttribute(FRAGMENT_COUNT_ATTRIBUTE);
        }
    }

    @Override
    protected boolean processBin(final Bin unmodifiableBin, final List<FlowFileSessionWrapper> binCopy, final ProcessContext context,
            final ProcessSession session) throws ProcessException {

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

        if (MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            final String error = getDefragmentValidationError(binCopy);

            // Fail the flow files and commit them
            if (error != null) {
                final String binDescription = binCopy.size() <= 10 ? binCopy.toString() : binCopy.size() + " FlowFiles";
                getLogger().error(error + "; routing {} to failure", new Object[]{binDescription});
                for (final FlowFileSessionWrapper wrapper : binCopy) {
                    wrapper.getSession().transfer(wrapper.getFlowFile(), REL_FAILURE);
                    wrapper.getSession().commit();
                }

                return true;
            }
            Collections.sort(binCopy, new FragmentComparator());
        }

        FlowFile bundle = merger.merge(context, session, binCopy);

        // keep the filename, as it is added to the bundle.
        final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

        // merge all of the attributes
        final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(binCopy);
        bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
        // restore the filename of the bundle
        bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
        bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(binCopy.size()));
        bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(unmodifiableBin.getBinAge()));

        bundle = session.putAllAttributes(bundle, bundleAttributes);

        final String inputDescription = binCopy.size() < 10 ? binCopy.toString() : binCopy.size() + " FlowFiles";
        getLogger().info("Merged {} into {}", new Object[]{inputDescription, bundle});
        session.transfer(bundle, REL_MERGED);

        // We haven't committed anything, parent will take care of it
        return false;
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
                return "Cannot Defragment " + flowFile + " because it is grouped with another FlowFile, and the two have differing values for the "
                        + FRAGMENT_COUNT_ATTRIBUTE + " attribute: " + decidedFragmentCount + " and " + fragmentCount;
            }
        }

        final int numericFragmentCount;
        try {
            numericFragmentCount = Integer.parseInt(decidedFragmentCount);
        } catch (final NumberFormatException nfe) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the " + FRAGMENT_COUNT_ATTRIBUTE + " has a non-integer value of " + decidedFragmentCount;
        }

        if (bin.size() < numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found only "
                    + bin.size() + " fragments";
        }

        if (bin.size() > numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found "
                    + bin.size() + " fragments for this identifier";
        }

        return null;
    }

    private boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        return NUMBER_PATTERN.matcher(value).matches();
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
                    final byte[] header = getDelimiterContent(context, wrappers, HEADER);
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
                            final byte[] demarcator = getDelimiterContent(context, wrappers, DEMARCATOR);
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

                    final byte[] footer = getDelimiterContent(context, wrappers, FOOTER);
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

        private byte[] getDelimiterContent(final ProcessContext context, final List<FlowFileSessionWrapper> wrappers, final PropertyDescriptor descriptor)
                throws IOException {
            final String delimiterStrategyValue = context.getProperty(DELIMITER_STRATEGY).getValue();
            if (DELIMITER_STRATEGY_FILENAME.equals(delimiterStrategyValue)) {
                return getDelimiterFileContent(context, wrappers, descriptor);
            } else {
                return getDelimiterTextContent(context, wrappers, descriptor);
            }
        }

        private byte[] getDelimiterFileContent(final ProcessContext context, final List<FlowFileSessionWrapper> wrappers, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            final String descriptorValue = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
            if (descriptorValue != null && wrappers != null && wrappers.size() > 0) {
                final String content = new String(readContent(descriptorValue));
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

        private byte[] getDelimiterTextContent(final ProcessContext context, final List<FlowFileSessionWrapper> wrappers, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            if (wrappers != null && wrappers.size() > 0) {
                final FlowFileSessionWrapper wrapper = wrappers.get(0);
                if (wrapper != null) {
                    final FlowFile flowFile = wrapper.getFlowFile();
                    if (flowFile != null) {
                        property = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue().getBytes();
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
            path = path.getNameCount() == 1 ? null : path.subpath(1, path.getNameCount());
        }

        return path == null ? "" : path.toString() + "/";
    }

    private String createFilename(final List<FlowFileSessionWrapper> wrappers) {
        if (wrappers.size() == 1) {
            return wrappers.get(0).getFlowFile().getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = wrappers.get(0).getFlowFile();
            final String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
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
