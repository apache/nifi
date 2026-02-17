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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"fork", "record", "content", "array", "stream", "event"})
@CapabilityDescription("This processor allows the user to fork a record into multiple records. The user must specify at least one "
        + "Record Path, as a dynamic property, pointing to a field of type ARRAY containing RECORD objects. The processor accepts "
        + "two modes: 'split' and 'extract'. In both modes, there is one record generated per element contained in the designated "
        + "array. In the 'split' mode, each generated record will preserve the same schema as given in the input but the array "
        + "will contain only one element. In the 'extract' mode, the element of the array must be of record type and will be the "
        + "generated record. Additionally, in the 'extract' mode, it is possible to specify if each generated record should contain "
        + "all the fields of the parent records from the root level to the extracted record. This assumes that the fields to add in "
        + "the record are defined in the schema of the Record Writer controller service. See examples in the additional details "
        + "documentation of this processor.")
@DynamicProperty(name = "Record Path property", value = "The Record Path value",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "A Record Path value, pointing to a field of type ARRAY containing RECORD objects")
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The generated FlowFile will have a 'record.count' attribute indicating "
            + "the number of records that were written to the FlowFile."),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type indicated by the Record Writer"),
    @WritesAttribute(attribute = "<Attributes from Record Writer>", description = "Any Attribute that the configured Record Writer "
            + "returns will be added to the FlowFile.")
})
public class ForkRecord extends AbstractProcessor {

    private volatile RecordPathCache recordPathCache = new RecordPathCache(25);

    static final AllowableValue MODE_EXTRACT = new AllowableValue("extract", "Extract",
        "Generated records will be the elements of the array");
    static final AllowableValue MODE_SPLIT = new AllowableValue("split", "Split",
        "Generated records will preserve the input schema and will contain a one-element array");

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies the forking mode of the processor")
            .allowableValues(MODE_EXTRACT, MODE_SPLIT)
            .defaultValue(MODE_SPLIT.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_PARENT_FIELDS = new PropertyDescriptor.Builder()
            .name("Include Parent Fields")
            .description("This parameter is only valid with the 'extract' mode. If set to true, all the fields "
                    + "from the root level to the given array will be added as fields of each element of the "
                    + "array to fork.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_WRITER,
            MODE,
            INCLUDE_PARENT_FIELDS
    );

    public static final Relationship REL_FORK = new Relationship.Builder()
            .name("fork")
            .description("The FlowFiles containing the forked records will be routed to this relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFiles will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case a FlowFile generates an error during the fork operation, it will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_FORK
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        Validator validator = new RecordPathValidator();

        Map<PropertyDescriptor, String> processorProperties = validationContext.getProperties();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = validationContext.getProperty(property).getValue();
                if (!validationContext.isExpressionLanguagePresent(dynamicValue)) {
                    results.add(validator.validate(property.getDisplayName(), dynamicValue, validationContext));
                }
            }
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<RecordPath> recordPaths = new ArrayList<>();
        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String path = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
                if (StringUtils.isNotBlank(path)) {
                    recordPaths.add(recordPathCache.getCompiled(context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue()));
                }
            }
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final boolean addParentFields = context.getProperty(INCLUDE_PARENT_FIELDS).asBoolean();
        final boolean isSplitMode = context.getProperty(MODE).getValue().equals(MODE_SPLIT.getValue());

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = original.getAttributes();

        final FlowFile outFlowFile = session.create(original);
        final AtomicInteger readCount = new AtomicInteger(0);
        final AtomicInteger writeCount = new AtomicInteger(0);

        try {

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        final Record firstRecord = reader.nextRecord();

                        final RecordSchema readerSchema = reader.getSchema();
                        final RecordSchema configuredWriterSchema = writerFactory.getSchema(originalAttributes, readerSchema);

                        // we compute the write schema only if the writer is configured to inherit the
                        // reader schema
                        final RecordSchema writeSchema;
                        if (configuredWriterSchema == readerSchema) {
                            final RecordSchema derivedSchema = determineWriteSchema(firstRecord, readerSchema);
                            writeSchema = writerFactory.getSchema(originalAttributes, derivedSchema);
                        } else {
                            writeSchema = configuredWriterSchema;
                        }

                        try (final OutputStream out = session.write(outFlowFile);
                                final RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), writeSchema, out, outFlowFile)) {

                            recordSetWriter.beginRecordSet();

                            if (firstRecord != null) {
                                writeForkedRecords(firstRecord, recordSetWriter, writeSchema);
                                readCount.incrementAndGet();
                            }

                            Record record;
                            while ((record = reader.nextRecord()) != null) {
                                readCount.incrementAndGet();
                                writeForkedRecords(record, recordSetWriter, writeSchema);
                            }

                            final WriteResult writeResult = recordSetWriter.finishRecordSet();

                            try {
                                recordSetWriter.close();
                            } catch (final IOException ioe) {
                                getLogger().warn("Failed to close Writer for {}", outFlowFile);
                            }

                            final Map<String, String> attributes = new HashMap<>();
                            writeCount.set(writeResult.getRecordCount());
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            session.transfer(session.putAllAttributes(outFlowFile, attributes), REL_FORK);
                        }

                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data: " + e.getLocalizedMessage(), e);
                    }
                }

                private RecordSchema determineWriteSchema(final Record firstRecord, final RecordSchema readerSchema) throws SchemaNotFoundException, IOException {
                    if (isSplitMode || firstRecord == null) {
                        return readerSchema;
                    }

                    final Map<String, RecordField> fieldMap = new LinkedHashMap<>();

                    for (RecordPath recordPath : recordPaths) {
                        final Iterator<FieldValue> iterator = recordPath.evaluate(firstRecord).getSelectedFields().iterator();
                        while (iterator.hasNext()) {
                            final FieldValue fieldValue = iterator.next();
                            Object fieldObject = fieldValue.getValue();
                            if (fieldObject instanceof List<?>) {
                                fieldObject = ((List<?>) fieldObject).toArray();
                            }

                            DataType dataType = fieldValue.getField().getDataType();
                            if (dataType.getFieldType() == RecordFieldType.CHOICE) {
                                DataType chosen = null;
                                if (fieldObject != null) {
                                    chosen = DataTypeUtils.chooseDataType(fieldObject, (ChoiceDataType) dataType);
                                }
                                if (chosen == null) {
                                    for (final DataType possible : ((ChoiceDataType) dataType).getPossibleSubTypes()) {
                                        if (((ArrayDataType) possible).getElementType().getFieldType() == RecordFieldType.RECORD) {
                                            chosen = possible;
                                            break;
                                        }
                                        if (chosen == null) {
                                            chosen = possible;
                                        }
                                    }
                                }
                                if (chosen != null) {
                                    dataType = chosen;
                                }
                            }

                            if (!(dataType instanceof ArrayDataType arrayDataType)) {
                                continue;
                            }

                            final DataType elementType = arrayDataType.getElementType();

                            if (elementType.getFieldType() != RecordFieldType.RECORD) {
                                continue;
                            }

                            final RecordSchema elementSchema = ((RecordDataType) elementType).getChildSchema();
                            for (final RecordField elementField : elementSchema.getFields()) {
                                fieldMap.put(elementField.getFieldName(), elementField);
                            }

                            if (addParentFields) {
                                addParentFieldSchemas(fieldMap, fieldValue);
                            }
                        }
                    }

                    final RecordSchema schema = new SimpleRecordSchema(new ArrayList<>(fieldMap.values()));
                    return writerFactory.getSchema(originalAttributes, schema);
                }

                private void addParentFieldSchemas(final Map<String, RecordField> fieldMap, final FieldValue fieldValue) {
                    try {
                        final FieldValue parentField = fieldValue.getParent().get();
                        final Record parentRecord = fieldValue.getParentRecord().get();

                        for (final RecordField field : parentRecord.getSchema().getFields()) {
                            if (!field.getFieldName().equals(fieldValue.getField().getFieldName())) {
                                fieldMap.putIfAbsent(field.getFieldName(), field);
                            }
                        }

                        addParentFieldSchemas(fieldMap, parentField);
                    } catch (NoSuchElementException e) {
                        return; // No parent field, nothing to do. NOTE: A return statement is needed to ensure this method does not recurse infinitely.
                    }
                }

                private void writeForkedRecords(final Record record, final RecordSetWriter recordSetWriter, final RecordSchema writeSchema) throws IOException {
                    for (RecordPath recordPath : recordPaths) {
                        final Iterator<FieldValue> it = recordPath.evaluate(record).getSelectedFields().iterator();

                        while (it.hasNext()) {
                            final FieldValue fieldValue = it.next();
                            Object fieldObject = fieldValue.getValue();
                            if (fieldObject instanceof List<?>) {
                                fieldObject = ((List<?>) fieldObject).toArray();
                            }

                            DataType dataType = fieldValue.getField().getDataType();
                            if (dataType.getFieldType() == RecordFieldType.CHOICE) {
                                DataType chosen = null;
                                if (fieldObject != null) {
                                    chosen = DataTypeUtils.chooseDataType(fieldObject, (ChoiceDataType) dataType);
                                }
                                if (chosen == null) {
                                    for (final DataType possible : ((ChoiceDataType) dataType).getPossibleSubTypes()) {
                                        if (possible.getFieldType() == RecordFieldType.ARRAY) {
                                            if (((ArrayDataType) possible).getElementType().getFieldType() == RecordFieldType.RECORD) {
                                                chosen = possible;
                                                break;
                                            }
                                            if (chosen == null) {
                                                chosen = possible;
                                            }
                                        }
                                    }
                                }
                                if (chosen != null) {
                                    dataType = chosen;
                                }
                            }

                            if (!(dataType instanceof ArrayDataType) || fieldObject == null) {
                                getLogger().debug("The record path {} is matching a field of type {} when the type ARRAY is expected.", recordPath.getPath(), dataType.getFieldType());
                                continue;
                            }

                            if (isSplitMode) {
                                final Object[] items = (Object[]) fieldObject;
                                for (final Object item : items) {
                                    fieldValue.updateValue(new Object[]{item});
                                    recordSetWriter.write(record);
                                }
                            } else {
                                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                                final DataType elementType = arrayDataType.getElementType();

                                if (elementType.getFieldType() != RecordFieldType.RECORD) {
                                    getLogger().debug("The record path {} is matching an array field with values of type {} when the type RECORD is expected.",
                                            recordPath.getPath(), elementType.getFieldType());
                                    continue;
                                }

                                final Object[] records = (Object[]) fieldObject;
                                for (final Object elementRecord : records) {
                                    if (elementRecord == null) {
                                        continue;
                                    }

                                    final Record recordToWrite = (Record) elementRecord;

                                    if (addParentFields) {
                                        recordToWrite.incorporateSchema(writeSchema);
                                        recursivelyAddParentFields(recordToWrite, fieldValue);
                                    }

                                    recordSetWriter.write(recordToWrite);
                                }
                            }
                        }
                    }
                }

                private void recursivelyAddParentFields(Record recordToWrite, FieldValue fieldValue) {
                    try {
                        // we get the parent data
                        FieldValue parentField = fieldValue.getParent().get();
                        Record parentRecord = fieldValue.getParentRecord().get();

                        // for each field of the parent
                        for (String field : parentRecord.getSchema().getFieldNames()) {
                            // if and only if there is not an already existing field with this name
                            // (we want to give priority to the deeper existing fields)
                            if (recordToWrite.getValue(field) == null) {
                                // Updates the value of the field with the given name to the given value.
                                // If the field specified is not present in the schema, will do nothing.
                                recordToWrite.setValue(field, parentRecord.getValue(field));
                            }
                        }

                        // recursive call
                        recursivelyAddParentFields(recordToWrite, parentField);
                    } catch (NoSuchElementException e) {
                        return; // NOTE: A return statement is needed to ensure this method does not recurse infinitely.
                    }
                }
            });

        } catch (Exception e) {
            getLogger().error("Failed to fork {}", flowFile, e);
            session.remove(outFlowFile);
            session.transfer(original, REL_FAILURE);
            return;
        }

        session.adjustCounter("Records Processed", readCount.get(), false);
        session.adjustCounter("Records Generated", writeCount.get(), false);
        getLogger().debug("Successfully forked {} records into {} records in {}", readCount.get(), writeCount.get(), flowFile);
        session.transfer(original, REL_ORIGINAL);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("record-reader", RECORD_READER.getName());
        config.renameProperty("record-writer", RECORD_WRITER.getName());
        config.renameProperty("fork-mode", MODE.getName());
        config.renameProperty("include-parent-fields", INCLUDE_PARENT_FIELDS.getName());
    }
}
