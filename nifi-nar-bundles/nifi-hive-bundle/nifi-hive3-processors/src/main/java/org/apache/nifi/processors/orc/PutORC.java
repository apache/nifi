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
package org.apache.nifi.processors.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.processors.orc.record.ORCHDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.hive.HiveUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "ORC", "hadoop", "HDFS", "filesystem", "restricted", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to a ORC file in the location/filesystem specified in the configuration.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute."),
        @WritesAttribute(attribute = "record.count", description = "The number of records written to the ORC file"),
        @WritesAttribute(attribute = "hive.ddl", description = "Creates a partial Hive DDL statement for creating an external table in Hive from the destination folder. "
                + "This can be used in ReplaceText for setting the content to the DDL. To make it valid DDL, add \"LOCATION '<path_to_orc_file_in_hdfs>'\", where "
                + "the path is the directory that contains this ORC file on HDFS. For example, this processor can send flow files downstream to ReplaceText to set the content "
                + "to this DDL (plus the LOCATION clause as described), then to PutHiveQL processor to create the table if it doesn't exist.")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                explanation = "Provides operator the ability to write to any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutORC extends AbstractPutHDFSRecord {

    public static final String HIVE_DDL_ATTRIBUTE = "hive.ddl";

    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("putorc-config-resources")
            .displayName("ORC Configuration Resources")
            .description("A file or comma separated list of files which contains the ORC configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Please see the ORC documentation for more details.")
            .required(false).addValidator(HiveUtils.createMultipleFilesExistValidator()).build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-stripe-size")
            .displayName("Stripe Size")
            .description("The size of the memory buffer (in bytes) for writing stripes to an ORC file")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("64 MB")
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-buffer-size")
            .displayName("Buffer Size")
            .description("The maximum size of the memory buffers (in bytes) used for compressing and storing a stripe in memory. This is a hint to the ORC writer, "
                    + "which may choose to use a smaller buffer size based on stripe size and number of columns for efficient stripe writing and memory utilization.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 KB")
            .build();

    static final PropertyDescriptor HIVE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("putorc-hive-table-name")
            .displayName("Hive Table Name")
            .description("An optional table name to insert into the hive.ddl attribute. The generated DDL can be used by "
                    + "a PutHive3QL processor (presumably after a PutHDFS processor) to create a table backed by the converted ORC file. "
                    + "If this property is not provided, the full name (including namespace) of the incoming Avro record will be normalized "
                    + "and used as the table name.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("putorc-hive-field-names")
            .displayName("Normalize Field Names for Hive")
            .description("Whether to normalize field names for Hive (force lowercase, e.g.). If the ORC file is going to "
                    + "be part of a Hive table, this property should be set to true. To preserve the original field names from the "
                    + "schema, this property should be set to false.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();


    public static final List<AllowableValue> COMPRESSION_TYPES;

    static {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        compressionTypes.add(new AllowableValue("NONE", "NONE", "No compression"));
        compressionTypes.add(new AllowableValue("ZLIB", "ZLIB", "ZLIB compression"));
        compressionTypes.add(new AllowableValue("SNAPPY", "SNAPPY", "Snappy compression"));
        compressionTypes.add(new AllowableValue("LZO", "LZO", "LZO compression"));
        COMPRESSION_TYPES = Collections.unmodifiableList(compressionTypes);
    }

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return "NONE";
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ORC_CONFIGURATION_RESOURCES);
        _propertyDescriptors.add(STRIPE_SIZE);
        _propertyDescriptors.add(BUFFER_SIZE);
        _propertyDescriptors.add(HIVE_TABLE_NAME);
        _propertyDescriptors.add(HIVE_FIELD_NAMES);
        return Collections.unmodifiableList(_propertyDescriptors);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final long stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
        final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final CompressionKind compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
        final boolean normalizeForHive = context.getProperty(HIVE_FIELD_NAMES).asBoolean();
        TypeInfo orcSchema = NiFiOrcUtils.getOrcSchema(schema, normalizeForHive);
        final Writer orcWriter = NiFiOrcUtils.createWriter(path, conf, orcSchema, stripeSize, compressionType, bufferSize);
        final String hiveTableName = context.getProperty(HIVE_TABLE_NAME).isSet()
                ? context.getProperty(HIVE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue()
                : NiFiOrcUtils.normalizeHiveTableName(schema.getIdentifier().getName().orElse("unknown"));
        final boolean hiveFieldNames = context.getProperty(HIVE_FIELD_NAMES).asBoolean();

        return new ORCHDFSRecordWriter(orcWriter, schema, hiveTableName, hiveFieldNames);
    }
}
