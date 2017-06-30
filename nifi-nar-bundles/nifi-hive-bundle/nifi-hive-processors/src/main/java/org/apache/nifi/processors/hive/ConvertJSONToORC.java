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
package org.apache.nifi.processors.hive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFlowFileWriter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.hive.HiveJdbcCommon;
import org.apache.nifi.util.hive.HiveUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


@Tags({"json", "orc"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts JSON files to ORC according to an Json Schema. ")
@WritesAttributes({
                      @WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/octet-stream"),
                      @WritesAttribute(attribute = "filename", description = "Sets the filename to the existing filename with the extension replaced by / added to by .orc"),
                      @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the ORC file.")
                  })
public class ConvertJSONToORC extends AbstractProcessor {

    private static String JSON_FIELD_NAME = "name";
    private static String JSON_FIELD_TYPE = "type";

    private volatile Configuration conf;
    private static ObjectMapper objectMapper;
    private static JsonFactory jsonFactory;

    // Attributes
    public static final String ORC_MIME_TYPE = "application/octet-stream";
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    // Relationship
    private static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("ORC content that was converted successfully from JSON")
        .build();

    private static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("JSON content that could not be processed")
        .build();

    // PropertyDescriptor
    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
        .name("orc-config-resources")
        .displayName("ORC Configuration Resources")
        .description("A file or comma separated list of files which contains the ORC configuration (hive-site.xml, e.g.). Without this, Hadoop "
                     + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Please see the ORC documentation for more details.")
        .required(false).addValidator(HiveUtils.createMultipleFilesExistValidator()).build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Json schema")
        .description("Outgoing ORC schema for each record created from a JSON object. "
                     + "ex) { \"fields\": [{ \"name\": \"valueName1\", \"type\": \"string\"}, { \"name\": \"valueName2\", \"type\": \"string\"}]}")
        .addValidator(schema_validator())
        .expressionLanguageSupported(true)
        .required(true)
        .build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
        .name("orc-stripe-size")
        .displayName("Stripe Size")
        .description("The size of the memory buffer (in bytes) for writing stripes to an ORC file")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("64 MB")
        .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("orc-buffer-size")
        .displayName("Buffer Size")
        .description("The maximum size of the memory buffers (in bytes) used for compressing and storing a stripe in memory. This is a hint to the ORC writer, "
                     + "which may choose to use a smaller buffer size based on stripe size and number of columns for efficient stripe writing and memory utilization.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("10 KB")
        .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
        .name("orc-compression-type")
        .displayName("Compression Type")
        .required(true)
        .allowableValues("NONE", "ZLIB", "SNAPPY", "LZO")
        .defaultValue("NONE")
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = ImmutableList.<PropertyDescriptor>builder()
        .add(ORC_CONFIGURATION_RESOURCES)
        .add(SCHEMA)
        .add(STRIPE_SIZE)
        .add(BUFFER_SIZE)
        .add(COMPRESSION_TYPE)
        .build();

    private static final Set<Relationship> RELATIONSHIPS = ImmutableSet.<Relationship>builder()
        .add(SUCCESS)
        .add(FAILURE)
        .build();

    public ConvertJSONToORC() {}

    // initialize Object
    static {
        objectMapper = new ObjectMapper();
        jsonFactory = objectMapper.getFactory();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // hadoop conf
        boolean confFileProvided = context.getProperty(ORC_CONFIGURATION_RESOURCES).isSet();
        if (confFileProvided) {
            final String configFiles = context.getProperty(ORC_CONFIGURATION_RESOURCES).getValue();
            conf = HiveJdbcCommon.getConfigurationFromFiles(configFiles);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)
        throws ProcessException {
        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            final long stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
            final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final CompressionKind compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
            final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final AtomicInteger totalRecordCount = new AtomicInteger(0);
            String schemaProperty = context.getProperty(SCHEMA)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
            // convert json-schema to orc-schema
            TypeInfo orcSchema = getSchema(schemaProperty);

            // convert json to orc
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    if (conf == null) {
                        conf = new Configuration();
                    }
                    // fields info
                    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(orcSchema);
                    List<String> fieldNames = new ArrayList<String>();
                    List<String> fieldTypes = new ArrayList<String>();
                    if (inspector instanceof StructObjectInspector) {
                        StructObjectInspector soi = (StructObjectInspector) inspector;
                        List<? extends StructField> fields = soi.getAllStructFieldRefs();
                        for (StructField field : fields) {
                            fieldNames.add(field.getFieldName());
                            fieldTypes.add(field.getFieldObjectInspector().getTypeName());
                        }
                    }
                    // orc write
                    OrcFlowFileWriter orcWriter = NiFiOrcUtils.createWriter(
                        out,
                        new Path(fileName),
                        conf,
                        orcSchema,
                        stripeSize,
                        compressionType,
                        bufferSize);
                    try (final JsonParser jsonParser = jsonFactory.createParser(in)) {
                        int recordCount = 0;
                        while (jsonParser.nextToken() != null) {
                            JsonNode obj = jsonParser.readValueAsTree();
                            Object[] row = new Object[fieldNames.size()];
                            for (int i = 0; i < fieldNames.size(); i++) {
                                row[i] = makeOrcObject(obj, fieldTypes.get(i), fieldNames.get(i));
                            }
                            orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, row));
                            recordCount++;
                        }
                        totalRecordCount.set(recordCount);
                    } catch (JsonParseException e1) {
                        throw new ProcessException("fail to parse json contents", e1);
                    } catch (IOException e2) {
                        throw new ProcessException("fail to wrtie contents", e2);
                    } finally {
                        orcWriter.close();
                    }
                }
            });

            // Rename a file
            StringBuilder newFilename = new StringBuilder();
            int extensionIndex = fileName.lastIndexOf(".");
            if (extensionIndex != -1) {
                newFilename.append(fileName.substring(0, extensionIndex));
            } else {
                newFilename.append(fileName);
            }
            newFilename.append(".orc");

            // Set attributes
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), newFilename.toString());
            flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE, Integer.toString(totalRecordCount.get()));
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), ORC_MIME_TYPE);
            logger.info("converted {} records. filename : {} ", new Object[] {totalRecordCount, newFilename});

            // Transfer - SUCCESS
            session.transfer(flowFile, SUCCESS);

        } catch (ProcessException e) {
            // Transfer - FAILURE
            logger.error("fail to convert. Error : {}", new Object[] {e});
            session.transfer(flowFile, FAILURE);
        }
    }

    public static TypeInfo getSchema(String schemaProperty) {
        JsonParser jsonParser;
        TypeInfo orcSchema;
        List<String> orcFieldNames = new ArrayList<>();
        List<TypeInfo> orcFields = new ArrayList<>();
        try {
            jsonParser = jsonFactory.createParser(schemaProperty);
            JsonNode node = jsonParser.readValueAsTree();
            Iterator<String> itr = node.fieldNames();
            JsonNode arr = null;
            while (itr.hasNext()) {
                JsonNode tmp = node.get(itr.next());
                if (tmp.isArray()) {
                    arr = tmp;
                    break;
                }
            }
            Iterator<JsonNode> arrItr = arr.elements();
            while (arrItr.hasNext()) {
                JsonNode arrNode = arrItr.next();
                if (arrNode.has(JSON_FIELD_NAME) && arrNode.has(JSON_FIELD_TYPE)) {
                    orcFieldNames.add(arrNode.get(JSON_FIELD_NAME).asText());
                    orcFields.add(TypeInfoFactory.getPrimitiveTypeInfo(arrNode.get(JSON_FIELD_TYPE).asText()));
                } else {
                    throw new ProcessException(JSON_FIELD_NAME + " and " + JSON_FIELD_TYPE + " are essential in schema");
                }
            }
        } catch (JsonParseException e1) {
            throw new ProcessException("invalid json schema.", e1);
        } catch (IOException e2) {
            throw new ProcessException("fail to read schema", e2);
        }
        orcSchema = TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
        return orcSchema;
    }

    public static Object makeOrcObject(JsonNode obj, String typeName, String fieldName) throws IOException {
        Object row;
        if (serdeConstants.STRING_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new Text(obj.get(fieldName).asText());
        } else if (serdeConstants.BOOLEAN_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new BooleanWritable(obj.get(fieldName).asBoolean());
        } else if (serdeConstants.INT_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new IntWritable(obj.get(fieldName).asInt());
        } else if (serdeConstants.BIGINT_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new LongWritable(obj.get(fieldName).asLong());
        } else if (serdeConstants.TINYINT_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new BytesWritable(obj.get(fieldName).binaryValue());
        } else if (serdeConstants.SMALLINT_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new ShortWritable(obj.get(fieldName).shortValue());
        } else if (serdeConstants.FLOAT_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new FloatWritable(obj.get(fieldName).floatValue());
        } else if (serdeConstants.DOUBLE_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new DoubleWritable(obj.get(fieldName).asDouble());
        } else if (serdeConstants.DATE_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new Text(obj.get(fieldName).asText());
        } else if (serdeConstants.TIMESTAMP_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new Text(obj.get(fieldName).asText());
        } else if (serdeConstants.BINARY_TYPE_NAME.equalsIgnoreCase(typeName)) {
            row = new BytesWritable(obj.get(fieldName).binaryValue());
        } else {
            row = new Text(obj.get(fieldName).asText());
        }
        return row;
    }

    public static final Validator schema_validator() {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                String message = "";
                try {
                    getSchema(input);
                } catch (ProcessException e) {
                    message = e.getMessage();
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };
    }
}
