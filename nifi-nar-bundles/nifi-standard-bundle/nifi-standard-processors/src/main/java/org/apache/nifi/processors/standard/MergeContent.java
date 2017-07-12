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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processors.standard.merge.AttributeStrategy;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV2;
import org.apache.nifi.util.FlowFilePackagerV3;

@SideEffectFree
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
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
@SeeAlso({SegmentContent.class, MergeRecord.class})
public class MergeContent extends BinFiles {

    // preferred attributes
    public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();

    // old style attributes
    public static final String SEGMENT_ID_ATTRIBUTE = "segment.identifier";
    public static final String SEGMENT_INDEX_ATTRIBUTE = "segment.index";
    public static final String SEGMENT_COUNT_ATTRIBUTE = "segment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();


    public static final AllowableValue METADATA_STRATEGY_USE_FIRST = new AllowableValue("Use First Metadata", "Use First Metadata",
            "For any input format that supports metadata (Avro, e.g.), the metadata for the first FlowFile in the bin will be set on the output FlowFile.");

    public static final AllowableValue METADATA_STRATEGY_ALL_COMMON = new AllowableValue("Keep Only Common Metadata", "Keep Only Common Metadata",
            "For any input format that supports metadata (Avro, e.g.), any FlowFile whose metadata values match those of the first FlowFile, any additional metadata "
                    + "will be dropped but the FlowFile will be merged. Any FlowFile whose metadata values do not match those of the first FlowFile in the bin will not be merged.");

    public static final AllowableValue METADATA_STRATEGY_IGNORE = new AllowableValue("Ignore Metadata", "Ignore Metadata",
            "Ignores (does not transfer, compare, etc.) any metadata from a FlowFile whose content supports embedded metadata.");

    public static final AllowableValue METADATA_STRATEGY_DO_NOT_MERGE = new AllowableValue("Do Not Merge Uncommon Metadata", "Do Not Merge Uncommon Metadata",
            "For any input format that supports metadata (Avro, e.g.), any FlowFile whose metadata values do not match those of the first FlowFile in the bin will not be merged.");

    public static final PropertyDescriptor METADATA_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("mergecontent-metadata-strategy")
            .displayName("Metadata Strategy")
            .description("For FlowFiles whose input format supports metadata (Avro, e.g.), this property determines which metadata should be added to the bundle. "
                    + "If 'Use First Metadata' is selected, the metadata keys/values from the first FlowFile to be bundled will be used. If 'Keep Only Common Metadata' is selected, "
                    + "only the metadata that exists on all FlowFiles in the bundle, with the same value, will be preserved. If 'Ignore Metadata' is selected, no metadata is transferred to "
                    + "the outgoing bundled FlowFile. If 'Do Not Merge Uncommon Metadata' is selected, any FlowFile whose metadata values do not match those of the first bundled FlowFile "
                    + "will not be merged.")
            .allowableValues(METADATA_STRATEGY_USE_FIRST, METADATA_STRATEGY_ALL_COMMON, METADATA_STRATEGY_DO_NOT_MERGE, METADATA_STRATEGY_IGNORE)
            .defaultValue(METADATA_STRATEGY_DO_NOT_MERGE.getValue())
            .build();

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
    public static final String MERGE_FORMAT_AVRO_VALUE = "Avro";

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
    public static final AllowableValue MERGE_FORMAT_AVRO = new AllowableValue(
            MERGE_FORMAT_AVRO_VALUE,
            MERGE_FORMAT_AVRO_VALUE,
            "The Avro contents of all FlowFiles will be concatenated together into a single FlowFile");


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
            .allowableValues(MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP, MERGE_FORMAT_FLOWFILE_STREAM_V3, MERGE_FORMAT_FLOWFILE_STREAM_V2, MERGE_FORMAT_FLOWFILE_TAR_V1, MERGE_FORMAT_CONCAT, MERGE_FORMAT_AVRO)
            .defaultValue(MERGE_FORMAT_CONCAT.getValue())
            .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Correlation Attribute Name")
            .description("If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for "
                    + "this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
            .required(false)
            .expressionLanguageSupported(true)
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
    public static final PropertyDescriptor TAR_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Tar Modified Time")
            .description("If using the Tar Merge Format, specifies if the Tar entry should store the modified timestamp either by expression "
                    + "(e.g. ${file.lastModifiedTime} or static value, both of which must match the ISO8601 format 'yyyy-MM-dd'T'HH:mm:ssZ'; if using "
                    + "other merge strategy or left blank, this value is ignored")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${file.lastModifiedTime}")
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
        descriptors.add(AttributeStrategyUtil.ATTRIBUTE_STRATEGY);
        descriptors.add(CORRELATION_ATTRIBUTE_NAME);
        descriptors.add(METADATA_STRATEGY);
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
        descriptors.add(TAR_MODIFIED_TIME);
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
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final ProcessSession session) {
        final String correlationAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
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
    protected boolean processBin(final Bin bin, final ProcessContext context) throws ProcessException {

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
            case MERGE_FORMAT_AVRO_VALUE:
                merger = new AvroMerge();
                break;
            default:
                throw new AssertionError();
        }

        final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(context);

        final List<FlowFile> contents = bin.getContents();
        final ProcessSession binSession = bin.getSession();

        if (MERGE_STRATEGY_DEFRAGMENT.equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            final String error = getDefragmentValidationError(bin.getContents());

            // Fail the flow files and commit them
            if (error != null) {
                final String binDescription = contents.size() <= 10 ? contents.toString() : contents.size() + " FlowFiles";
                getLogger().error(error + "; routing {} to failure", new Object[]{binDescription});
                binSession.transfer(contents, REL_FAILURE);
                binSession.commit();

                return true;
            }

            Collections.sort(contents, new FragmentComparator());
        }

        FlowFile bundle = merger.merge(bin, context);

        // keep the filename, as it is added to the bundle.
        final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

        // merge all of the attributes
        final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(contents);
        bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
        // restore the filename of the bundle
        bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
        bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(contents.size()));
        bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));

        bundle = binSession.putAllAttributes(bundle, bundleAttributes);

        final String inputDescription = contents.size() < 10 ? contents.toString() : contents.size() + " FlowFiles";
        getLogger().info("Merged {} into {}", new Object[]{inputDescription, bundle});
        binSession.transfer(bundle, REL_MERGED);

        for (final FlowFile unmerged : merger.getUnmergedFlowFiles()) {
            final FlowFile unmergedCopy = binSession.clone(unmerged);
            binSession.transfer(unmergedCopy, REL_FAILURE);
        }

        // We haven't committed anything, parent will take care of it
        return false;
    }

    private String getDefragmentValidationError(final List<FlowFile> binContents) {
        if (binContents.isEmpty()) {
            return null;
        }

        // If we are defragmenting, all fragments must have the appropriate attributes.
        String decidedFragmentCount = null;
        String fragmentIdentifier = null;
        for (final FlowFile flowFile : binContents) {
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

        if (binContents.size() < numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found only "
                + binContents.size() + " fragments";
        }

        if (binContents.size() > numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found "
                + binContents.size() + " fragments for this identifier";
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
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();

            final ProcessSession session = bin.getSession();
            FlowFile bundle = session.create(bin.getContents());
            final AtomicReference<String> bundleMimeTypeRef = new AtomicReference<>(null);
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    final byte[] header = getDelimiterContent(context, contents, HEADER);
                    if (header != null) {
                        out.write(header);
                    }

                    boolean isFirst = true;
                    final Iterator<FlowFile> itr = contents.iterator();
                    while (itr.hasNext()) {
                        final FlowFile flowFile = itr.next();
                        bin.getSession().read(flowFile, false, new InputStreamCallback() {
                            @Override
                            public void process(final InputStream in) throws IOException {
                                StreamUtils.copy(in, out);
                            }
                        });

                        if (itr.hasNext()) {
                            final byte[] demarcator = getDelimiterContent(context, contents, DEMARCATOR);
                            if (demarcator != null) {
                                out.write(demarcator);
                            }
                        }

                        final String flowFileMimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
                        if (isFirst) {
                            bundleMimeTypeRef.set(flowFileMimeType);
                            isFirst = false;
                        } else {
                            if (bundleMimeTypeRef.get() != null && !bundleMimeTypeRef.get().equals(flowFileMimeType)) {
                                bundleMimeTypeRef.set(null);
                            }
                        }
                    }

                    final byte[] footer = getDelimiterContent(context, contents, FOOTER);
                    if (footer != null) {
                        out.write(footer);
                    }
                }
            });

            session.getProvenanceReporter().join(contents, bundle);
            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents));
            if (bundleMimeTypeRef.get() != null) {
                this.mimeType = bundleMimeTypeRef.get();
            }

            return bundle;
        }

        private byte[] getDelimiterContent(final ProcessContext context, final List<FlowFile> wrappers, final PropertyDescriptor descriptor) throws IOException {
            final String delimiterStrategyValue = context.getProperty(DELIMITER_STRATEGY).getValue();
            if (DELIMITER_STRATEGY_FILENAME.equals(delimiterStrategyValue)) {
                return getDelimiterFileContent(context, wrappers, descriptor);
            } else {
                return getDelimiterTextContent(context, wrappers, descriptor);
            }
        }

        private byte[] getDelimiterFileContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            final String descriptorValue = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
            if (descriptorValue != null && flowFiles != null && flowFiles.size() > 0) {
                final String content = new String(readContent(descriptorValue), StandardCharsets.UTF_8);
                final FlowFile flowFile = flowFiles.get(0);
                if (flowFile != null && content != null) {
                    final PropertyValue propVal = context.newPropertyValue(content).evaluateAttributeExpressions(flowFile);
                    property = propVal.getValue().getBytes(StandardCharsets.UTF_8);
                }
            }
            return property;
        }

        private byte[] getDelimiterTextContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            if (flowFiles != null && flowFiles.size() > 0) {
                final FlowFile flowFile = flowFiles.get(0);
                if (flowFile != null) {
                    final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                    if (value != null) {
                        property = value.getBytes(StandardCharsets.UTF_8);
                    }
                }
            }
            return property;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
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

    private String createFilename(final List<FlowFile> flowFiles) {
        if (flowFiles.size() == 1) {
            return flowFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = flowFiles.get(0);
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
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();
            final ProcessSession session = bin.getSession();

            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();
            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".tar");
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final TarArchiveOutputStream out = new TarArchiveOutputStream(bufferedOut)) {
                        out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                        for (final FlowFile flowFile : contents) {
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

                            final String modTime = context.getProperty(TAR_MODIFIED_TIME)
                                    .evaluateAttributeExpressions(flowFile).getValue();
                            if (StringUtils.isNotBlank(modTime)) {
                                try {
                                    tarEntry.setModTime(Instant.parse(modTime).toEpochMilli());
                                } catch (final Exception e) {
                                    getLogger().debug("Attribute {} of {} is set to {}; expected ISO8601 format, so ignoring",
                                            new Object[]{TAR_MODIFIED_TIME, flowFile, modTime});
                                }
                            }

                            out.putArchiveEntry(tarEntry);

                            bin.getSession().exportTo(flowFile, out);
                            out.closeArchiveEntry();
                        }
                    }
                }
            });

            bin.getSession().getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/tar";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
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
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            FlowFile bundle = session.create(contents);

            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut)) {
                        // we don't want the packager closing the stream. V1 creates a TAR Output Stream, which then gets
                        // closed, which in turn closes the underlying OutputStream, and we want to protect ourselves against that.
                        final OutputStream out = new NonCloseableOutputStream(bufferedOut);

                        for (final FlowFile flowFile : contents) {
                            bin.getSession().read(flowFile, false, new InputStreamCallback() {
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

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".pkg");
            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private class ZipMerge implements MergeBin {

        private final int compressionLevel;

        private List<FlowFile> unmerged = new ArrayList<>();

        public ZipMerge(final int compressionLevel) {
            this.compressionLevel = compressionLevel;
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();

            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();
            unmerged.addAll(contents);

            FlowFile bundle = session.create(contents);

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".zip");
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final ZipOutputStream out = new ZipOutputStream(bufferedOut)) {
                        out.setLevel(compressionLevel);
                        for (final FlowFile flowFile : contents) {
                            final String path = keepPath ? getPath(flowFile) : "";
                            final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());
                            final ZipEntry zipEntry = new ZipEntry(entryName);
                            zipEntry.setSize(flowFile.getSize());
                            try {
                                out.putNextEntry(zipEntry);

                                bin.getSession().exportTo(flowFile, out);
                                out.closeEntry();
                                unmerged.remove(flowFile);
                            } catch (ZipException e) {
                                getLogger().error("Encountered exception merging {}", new Object[]{flowFile}, e);
                            }
                        }

                        out.finish();
                        out.flush();
                    }
                }
            });

            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/zip";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }

    private class AvroMerge implements MergeBin {

        private List<FlowFile> unmerged = new ArrayList<>();

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            final String metadataStrategy = context.getProperty(METADATA_STRATEGY).getValue();
            final Map<String, byte[]> metadata = new TreeMap<>();
            final AtomicReference<Schema> schema = new AtomicReference<>(null);
            final AtomicReference<String> inputCodec = new AtomicReference<>(null);
            final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());

            // we don't pass the parents to the #create method because the parents belong to different sessions
            FlowFile bundle = session.create(contents);
            bundle = session.write(bundle, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream rawOut) throws IOException {
                    try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                        for (final FlowFile flowFile : contents) {
                            bin.getSession().read(flowFile, false, new InputStreamCallback() {
                                @Override
                                public void process(InputStream in) throws IOException {
                                    boolean canMerge = true;
                                    try (DataFileStream<GenericRecord> reader = new DataFileStream<>(in,
                                            new GenericDatumReader<GenericRecord>())) {
                                        if (schema.get() == null) {
                                            // this is the first file - set up the writer, and store the
                                            // Schema & metadata we'll use.
                                            schema.set(reader.getSchema());
                                            if (!METADATA_STRATEGY_IGNORE.getValue().equals(metadataStrategy)) {
                                                for (String key : reader.getMetaKeys()) {
                                                    if (!DataFileWriter.isReservedMeta(key)) {
                                                        byte[] metadatum = reader.getMeta(key);
                                                        metadata.put(key, metadatum);
                                                        writer.setMeta(key, metadatum);
                                                    }
                                                }
                                            }
                                            inputCodec.set(reader.getMetaString(DataFileConstants.CODEC));
                                            if (inputCodec.get() == null) {
                                                inputCodec.set(DataFileConstants.NULL_CODEC);
                                            }
                                            writer.setCodec(CodecFactory.fromString(inputCodec.get()));
                                            writer.create(schema.get(), out);
                                        } else {
                                            // check that we're appending to the same schema
                                            if (!schema.get().equals(reader.getSchema())) {
                                                getLogger().debug("Input file {} has different schema - {}, not merging",
                                                        new Object[]{flowFile.getId(), reader.getSchema().getName()});
                                                canMerge = false;
                                                unmerged.add(flowFile);
                                            }

                                            if (METADATA_STRATEGY_DO_NOT_MERGE.getValue().equals(metadataStrategy)
                                                    || METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy)) {
                                                // check that we're appending to the same metadata
                                                for (String key : reader.getMetaKeys()) {
                                                    if (!DataFileWriter.isReservedMeta(key)) {
                                                        byte[] metadatum = reader.getMeta(key);
                                                        byte[] writersMetadatum = metadata.get(key);
                                                        if (!Arrays.equals(metadatum, writersMetadatum)) {
                                                            // Ignore additional metadata if ALL_COMMON is the strategy, otherwise don't merge
                                                            if (!METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy) || writersMetadatum != null) {
                                                                getLogger().debug("Input file {} has different non-reserved metadata, not merging",
                                                                        new Object[]{flowFile.getId()});
                                                                canMerge = false;
                                                                unmerged.add(flowFile);
                                                            }
                                                        }
                                                    }
                                                }
                                            } // else the metadata in the first FlowFile was either ignored or retained in the if-clause above

                                            // check that we're appending to the same codec
                                            String thisCodec = reader.getMetaString(DataFileConstants.CODEC);
                                            if (thisCodec == null) {
                                                thisCodec = DataFileConstants.NULL_CODEC;
                                            }
                                            if (!inputCodec.get().equals(thisCodec)) {
                                                getLogger().debug("Input file {} has different codec, not merging",
                                                        new Object[]{flowFile.getId()});
                                                canMerge = false;
                                                unmerged.add(flowFile);
                                            }
                                        }

                                        // write the Avro content from the current FlowFile to the merged OutputStream
                                        if (canMerge) {
                                            writer.appendAllFrom(reader, false);
                                        }
                                    }
                                }
                            });
                        }
                        writer.flush();
                    } finally {
                        writer.close();
                    }
                }
            });

            final Collection<FlowFile> parents;
            if (unmerged.isEmpty()) {
                parents = contents;
            } else {
                parents = new HashSet<>(contents);
                parents.removeAll(unmerged);
            }

            session.getProvenanceReporter().join(parents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/avro-binary";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }



    private static class FragmentComparator implements Comparator<FlowFile> {

        @Override
        public int compare(final FlowFile o1, final FlowFile o2) {
            final int fragmentIndex1 = Integer.parseInt(o1.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            final int fragmentIndex2 = Integer.parseInt(o2.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            return Integer.compare(fragmentIndex1, fragmentIndex2);
        }
    }

    private interface MergeBin {

        FlowFile merge(Bin bin, ProcessContext context);

        String getMergedContentType();

        List<FlowFile> getUnmergedFlowFiles();
    }

}
