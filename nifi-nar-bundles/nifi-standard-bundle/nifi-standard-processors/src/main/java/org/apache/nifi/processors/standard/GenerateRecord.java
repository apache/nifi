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

import com.github.javafaker.Faker;
import com.github.javafaker.service.files.EnFile;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SupportsBatching
@Tags({"test", "random", "generate", "fake"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
})
@CapabilityDescription("This processor creates FlowFiles with records having random value for the specified fields. GenerateFakeRecord is useful " +
        "for testing, configuration, and simulation. It uses either user-defined properties to define a record schema or a provided schema and generates the specified number of records using " +
        "random data for the fields in the schema.")
public class GenerateRecord extends AbstractProcessor {

    // Additional Faker datatypes that don't use predetermined data files (i.e. they generate data or have non-String types)
    static final AllowableValue FT_BOOL = new AllowableValue("Boolean.bool", "Boolean - bool (true/false)", "A value of 'true' or 'false'");
    static final AllowableValue FT_FUTURE_DATE = new AllowableValue("DateAndTime.futureDate", "Date And Time - Future Date", "Generates a date up to one year in the " +
            "future from the time the processor is executed");
    static final AllowableValue FT_PAST_DATE = new AllowableValue("DateAndTime.pastDate", "Date And Time - Past Date", "Generates a date up to one year in the past from the time the " +
            "processor is executed");
    static final AllowableValue FT_BIRTHDAY = new AllowableValue("DateAndTime.birthday", "Date And Time - Birthday", "Generates a random birthday between 65 and 18 years ago");
    static final AllowableValue FT_MD5 = new AllowableValue("Crypto.MD5", "Crypto - MD5", "An MD5 hash");
    static final AllowableValue FT_NUMBER = new AllowableValue("Number.Integer", "Number - Integer", "A integer number");
    static final AllowableValue FT_SHA1 = new AllowableValue("Crypto.SHA-1", "Crypto - SHA-1", "A SHA-1 hash");
    static final AllowableValue FT_SHA256 = new AllowableValue("Crypto.SHA-256", "Crypto - SHA-256", "A SHA-256 hash");
    static final AllowableValue FT_SHA512 = new AllowableValue("Crypto.SHA-512", "Crypto - SHA-512", "A SHA-512 hash");

    static final String FT_LATITUDE_ALLOWABLE_VALUE_NAME = "Address.latitude";
    static final String FT_LONGITUDE_ALLOWABLE_VALUE_NAME = "Address.longitude";

    static final String[] SUPPORTED_LOCALES = {
            "bg",
            "ca",
            "ca-CAT",
            "da-DK",
            "de",
            "de-AT",
            "de-CH",
            "en",
            "en-AU",
            "en-au-ocker",
            "en-BORK",
            "en-CA",
            "en-GB",
            "en-IND",
            "en-MS",
            "en-NEP",
            "en-NG",
            "en-NZ",
            "en-PAK",
            "en-SG",
            "en-UG",
            "en-US",
            "en-ZA",
            "es",
            "es-MX",
            "fa",
            "fi-FI",
            "fr",
            "he",
            "hu",
            "in-ID",
            "it",
            "ja",
            "ko",
            "nb-NO",
            "nl",
            "pl",
            "pt",
            "pt-BR",
            "ru",
            "sk",
            "sv",
            "sv-SE",
            "tr",
            "uk",
            "vi",
            "zh-CN",
            "zh-TW"
    };

    private static final String PACKAGE_PREFIX = "com.github.javafaker";

    private volatile Faker faker = new Faker();

    private static final AllowableValue[] fakerDatatypeValues;

    protected static final Map<String, FakerMethodHolder> datatypeFunctionMap = new HashMap<>();

    static {
        final List<EnFile> fakerFiles = EnFile.getFiles();
        final Map<String, Class<?>> possibleFakerTypeMap = new HashMap<>(fakerFiles.size());
        for (EnFile fakerFile : fakerFiles) {
            String className = normalizeClassName(fakerFile.getFile().substring(0, fakerFile.getFile().indexOf('.')));
            try {
                possibleFakerTypeMap.put(className, Class.forName(PACKAGE_PREFIX + '.' + className));
            } catch (Exception e) {
                // Ignore, these are the ones we want to filter out
            }
        }

        // Filter on no-arg methods that return a String, these should be the methods the user can use to generate data
        Faker tempFaker = new Faker();
        List<AllowableValue> fakerDatatypeValueList = new ArrayList<>();
        for (Map.Entry<String, Class<?>> entry : possibleFakerTypeMap.entrySet()) {
            List<Method> fakerMethods = Arrays.stream(entry.getValue().getDeclaredMethods()).filter((method) ->
                            Modifier.isPublic(method.getModifiers())
                                    && method.getParameterCount() == 0
                                    && method.getReturnType() == String.class)
                    .collect(Collectors.toList());
            try {
                final Object methodObject = tempFaker.getClass().getDeclaredMethod(normalizeMethodName(entry.getKey())).invoke(tempFaker);
                for (Method method : fakerMethods) {
                    final String allowableValueName = normalizeClassName(entry.getKey()) + "." + method.getName();
                    final String allowableValueDisplayName = normalizeDisplayName(entry.getKey()) + " - " + normalizeDisplayName(method.getName());
                    datatypeFunctionMap.put(allowableValueName, new FakerMethodHolder(allowableValueName, methodObject, method));
                    fakerDatatypeValueList.add(new AllowableValue(allowableValueName, allowableValueDisplayName, allowableValueDisplayName));
                }
            } catch (Exception e) {
                // Ignore, this should indicate a Faker method that we're not interested in
            }
        }

        // Add types manually for those Faker methods that generate data rather than getting it from a resource file
        fakerDatatypeValueList.add(FT_FUTURE_DATE);
        fakerDatatypeValueList.add(FT_PAST_DATE);
        fakerDatatypeValueList.add(FT_BIRTHDAY);
        fakerDatatypeValueList.add(FT_NUMBER);
        fakerDatatypeValueList.add(FT_MD5);
        fakerDatatypeValueList.add(FT_SHA1);
        fakerDatatypeValueList.add(FT_SHA256);
        fakerDatatypeValueList.add(FT_SHA512);
        fakerDatatypeValues = fakerDatatypeValueList.toArray(new AllowableValue[]{});
    }

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
            .name("generate-record-schema-text")
            .displayName("Schema Text")
            .description("The text of an Avro-formatted Schema used to generate record data. If this property is set, any user-defined properties are ignored.")
            .addValidator(new AvroSchemaValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("generate-record-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor NUM_RECORDS = new PropertyDescriptor.Builder()
            .name("generate-record--num-records")
            .displayName("Number of Records")
            .description("Specifies how many records will be generated for each outgoing FlowFile.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor LOCALE = new PropertyDescriptor.Builder()
            .name("generate-record-locale")
            .displayName("Locale")
            .description("The locale that will be used to generate field data. For example a Locale of 'es' will generate fields (e.g. names) in Spanish.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("en-US")
            .allowableValues(SUPPORTED_LOCALES)
            .build();

    static final PropertyDescriptor NULLABLE_FIELDS = new PropertyDescriptor.Builder()
            .name("generate-record-nullable-fields")
            .displayName("Nullable Fields")
            .description("Whether the generated fields will be nullable. Note that this property is ignored if Schema Text is set. Also it only affects the schema of the generated data, " +
                    "not whether any values will be null. If this property is true, see 'Null Value Percentage' to set the probability that any generated field will be null.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    static final PropertyDescriptor NULL_PERCENTAGE = new PropertyDescriptor.Builder()
            .name("generate-record-null-pct")
            .displayName("Null Value Percentage")
            .description("The percent probability (0-100%) that a generated value for any nullable field will be null. Set this property to zero to have no null values, or 100 to have all " +
                    "null values.")
            .addValidator(StandardValidators.createLongValidator(0L, 100L, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .defaultValue("0")
            .dependsOn(NULLABLE_FIELDS, "true")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_TEXT);
        properties.add(RECORD_WRITER);
        properties.add(NUM_RECORDS);
        properties.add(LOCALE);
        properties.add(NULLABLE_FIELDS);
        properties.add(NULL_PERCENTAGE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .allowableValues(fakerDatatypeValues)
                .defaultValue("Address.fullAddress")
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String locale = context.getProperty(LOCALE).getValue();
        faker = new Faker(new Locale(locale));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions().getValue();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int numRecords = context.getProperty(NUM_RECORDS).evaluateAttributeExpressions().asInteger();
        FlowFile flowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        try {
            flowFile = session.write(flowFile, out -> {
                final RecordSchema recordSchema;
                final boolean usingSchema;
                final int nullPercentage = context.getProperty(NULL_PERCENTAGE).evaluateAttributeExpressions().asInteger();
                if (!StringUtils.isEmpty(schemaText)) {
                    final Schema avroSchema = new Schema.Parser().parse(schemaText);
                    recordSchema = AvroTypeUtil.createSchema(avroSchema);
                    usingSchema = true;
                } else {
                    // Generate RecordSchema from user-defined properties
                    final boolean nullable = context.getProperty(NULLABLE_FIELDS).asBoolean();
                    final Map<String, String> fields = getFields(context);
                    recordSchema = generateRecordSchema(fields, nullable);
                    usingSchema = false;
                }
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(attributes, recordSchema);
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                        writer.beginRecordSet();

                        Record record;
                        List<RecordField> writeFieldNames = writeSchema.getFields();
                        Map<String, Object> recordEntries = new HashMap<>();
                        for (int i = 0; i < numRecords; i++) {
                            for (RecordField writeRecordField : writeFieldNames) {
                                final String writeFieldName = writeRecordField.getFieldName();
                                final Object writeFieldValue;
                                if (usingSchema) {
                                    writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                                } else {
                                    final boolean nullValue;
                                    if (!context.getProperty(GenerateRecord.NULLABLE_FIELDS).asBoolean() || nullPercentage == 0) {
                                        nullValue = false;
                                    } else {
                                        nullValue = (faker.number().numberBetween(0, 100) <= nullPercentage);
                                    }
                                    if (nullValue) {
                                        writeFieldValue = null;
                                    } else {
                                        final String propertyValue = context.getProperty(writeFieldName).getValue();
                                        writeFieldValue = getFakeData(propertyValue, faker);
                                    }
                                }

                                recordEntries.put(writeFieldName, writeFieldValue);
                            }
                            record = new MapRecord(recordSchema, recordEntries);
                            writer.write(record);
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                        recordCount.set(writeResult.getRecordCount());
                    }
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException(e.getLocalizedMessage(), e);
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", flowFile, e);
            if (e instanceof ProcessException) {
                throw e;
            } else {
                throw new ProcessException(e);
            }
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);

        getLogger().info("Successfully generated {} records for {}", count, flowFile);
    }

    protected Map<String, String> getFields(ProcessContext context) {
        return context.getProperties().entrySet().stream()
                // filter non-null dynamic properties
                .filter(e -> e.getKey().isDynamic() && e.getValue() != null)
                // convert to Map of user-defined field names and types
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).getValue()
                ));
    }

    private Object getFakeData(String type, Faker faker) {

        // Catch these two cases ahead of calling the "discovered" Faker method below in order to return a double instead of a String
        if (FT_LATITUDE_ALLOWABLE_VALUE_NAME.equals(type)) {
            return Double.valueOf(faker.address().latitude());
        }
        if (FT_LONGITUDE_ALLOWABLE_VALUE_NAME.equals(type)) {
            return Double.valueOf(faker.address().longitude());
        }

        // Handle Number method not discovered by programmatically getting methods from the Faker objects
        if (FT_NUMBER.getValue().equals(type)) {
            return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        // Handle DateAndTime methods not discovered by programmatically getting methods from the Faker objects
        if (FT_FUTURE_DATE.getValue().equals(type)) {
            return faker.date().future(365, TimeUnit.DAYS);
        }
        if (FT_PAST_DATE.getValue().equals(type)) {
            return faker.date().past(365, TimeUnit.DAYS);
        }
        if (FT_BIRTHDAY.getValue().equals(type)) {
            return faker.date().birthday();
        }

        // Handle Crypto methods not discovered by programmatically getting methods from the Faker objects
        if (FT_MD5.getValue().equals(type)) {
            return faker.crypto().md5();
        }
        if (FT_SHA1.getValue().equals(type)) {
            return faker.crypto().sha1();
        }
        if (FT_SHA256.getValue().equals(type)) {
            return faker.crypto().sha256();
        }
        if (FT_SHA512.getValue().equals(type)) {
            return faker.crypto().sha512();
        }

        // If not a special circumstance, use the map to call the associated Faker method and return the value
        try {
            final FakerMethodHolder fakerMethodHolder = datatypeFunctionMap.get(type);
            Object returnObject = fakerMethodHolder.getMethod().invoke(fakerMethodHolder.getMethodObject());
            return returnObject;
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new ProcessException(type + " is not a valid value");
        }
    }

    // This method overrides the default String type for certain Faker datatypes for more user-friendly values (such as a Double for latitude/longitude)
    private DataType getDataType(final String type) {

        if (FT_FUTURE_DATE.getValue().equals(type)
                || FT_PAST_DATE.getValue().equals(type)
                || FT_BIRTHDAY.getValue().equals(type)
        ) {
            return RecordFieldType.DATE.getDataType();
        }
        if (FT_LATITUDE_ALLOWABLE_VALUE_NAME.equals(type)
                || FT_LONGITUDE_ALLOWABLE_VALUE_NAME.equals(type)) {
            return RecordFieldType.DOUBLE.getDataType("%.8g");
        }
        if (FT_NUMBER.getValue().equals(type)) {
            return RecordFieldType.INT.getDataType();
        }
        if (FT_BOOL.getValue().equals(type)) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        return RecordFieldType.STRING.getDataType();
    }

    private Object generateValueFromRecordField(RecordField recordField, Faker faker, int nullPercentage) {
        if (recordField.isNullable() && faker.number().numberBetween(0, 100) <= nullPercentage) {
            return null;
        }
        switch (recordField.getDataType().getFieldType()) {
            case BIGINT:
                return new BigInteger(String.valueOf(faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE)));
            case BOOLEAN:
                return getFakeData("Bool.bool", faker);
            case BYTE:
                return faker.number().numberBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
            case CHAR:
                return (char) faker.number().numberBetween(Character.MIN_VALUE, Character.MAX_VALUE);
            case DATE:
                return getFakeData(FT_PAST_DATE.getValue(), faker);
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
                return faker.number().randomDouble(((DecimalDataType) recordField.getDataType()).getScale(), Long.MIN_VALUE, Long.MAX_VALUE);
            case INT:
                return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case LONG:
                return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            case SHORT:
                return faker.number().numberBetween(Short.MIN_VALUE, Short.MAX_VALUE);
            case ENUM:
                List<String> enums = ((EnumDataType) recordField.getDataType()).getEnums();
                return enums.get(faker.number().numberBetween(0, enums.size() - 1));
            case TIME:
                DateFormat df = new SimpleDateFormat("HH:mm:ss");
                return df.format((Date) getFakeData(FT_PAST_DATE.getValue(), faker));
            case TIMESTAMP:
                return ((Date) getFakeData(FT_PAST_DATE.getValue(), faker)).getTime();
            case UUID:
                return UUID.randomUUID();
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) recordField.getDataType();
                final DataType elementType = arrayDataType.getElementType();
                final int numElements = faker.number().numberBetween(0, 10);
                Object[] returnValue = new Object[numElements];
                for (int i = 0; i < numElements; i++) {
                    RecordField tempRecordField = new RecordField(recordField.getFieldName() + "[" + i + "]", elementType, arrayDataType.isElementsNullable());
                    // If the array elements are non-nullable, use zero as the nullPercentage
                    returnValue[i] = generateValueFromRecordField(tempRecordField, faker, arrayDataType.isElementsNullable() ? nullPercentage : 0);
                }
                return returnValue;
            case MAP:
                final MapDataType mapDataType = (MapDataType) recordField.getDataType();
                final DataType valueType = mapDataType.getValueType();
                // Create 4-element fake map
                Map<String, Object> returnMap = new HashMap<>(4);
                returnMap.put("test1", generateValueFromRecordField(new RecordField("test1", valueType), faker, nullPercentage));
                returnMap.put("test2", generateValueFromRecordField(new RecordField("test2", valueType), faker, nullPercentage));
                returnMap.put("test3", generateValueFromRecordField(new RecordField("test3", valueType), faker, nullPercentage));
                returnMap.put("test4", generateValueFromRecordField(new RecordField("test4", valueType), faker, nullPercentage));
                return returnMap;
            case RECORD:
                final RecordDataType recordType = (RecordDataType) recordField.getDataType();
                final RecordSchema childSchema = recordType.getChildSchema();
                final Map<String, Object> recordValues = new HashMap<>();
                for (RecordField writeRecordField : childSchema.getFields()) {
                    final String writeFieldName = writeRecordField.getFieldName();
                    final Object writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                    recordValues.put(writeFieldName, writeFieldValue);
                }
                return new MapRecord(childSchema, recordValues);
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) recordField.getDataType();
                List<DataType> subTypes = choiceDataType.getPossibleSubTypes();
                // Pick one at random and generate a value for it
                DataType chosenType = subTypes.get(faker.number().numberBetween(0, subTypes.size() - 1));
                RecordField tempRecordField = new RecordField(recordField.getFieldName(), chosenType);
                return generateValueFromRecordField(tempRecordField, faker, nullPercentage);
            case STRING:
            default:
                return generateRandomString();
        }
    }

    private String generateRandomString() {
        final int categoryChoice = faker.number().numberBetween(0, 10);
        switch (categoryChoice) {
            case 0:
                return faker.name().fullName();
            case 1:
                return faker.lorem().word();
            case 2:
                return faker.shakespeare().romeoAndJulietQuote();
            case 3:
                return faker.educator().university();
            case 4:
                return faker.zelda().game();
            case 5:
                return faker.company().name();
            case 6:
                return faker.chuckNorris().fact();
            case 7:
                return faker.book().title();
            case 8:
                return faker.dog().breed();
            default:
                return faker.animal().name();
        }
    }

    protected RecordSchema generateRecordSchema(final Map<String, String> fields, final boolean nullable) {
        final List<RecordField> recordFields = new ArrayList<>(fields.size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            final DataType fieldDataType = getDataType(fieldType);
            RecordField recordField = new RecordField(fieldName, fieldDataType, nullable);
            recordFields.add(recordField);
        }
        return new SimpleRecordSchema(recordFields);
    }

    // This method identifies "segments" by splitting the given name on underscores, then capitalizes each segment and removes the underscores. Ex: 'game_of_thrones' = 'GameOfThrones'
    private static String normalizeClassName(String name) {
        String[] segments = name.split("_");
        String newName = Arrays.stream(segments).map((s) -> s.substring(0, 1).toUpperCase() + s.substring(1)).collect(Collectors.joining());
        return newName;
    }

    // This method lowercases the first letter of the given name in order to match the name to a Faker method
    private static String normalizeMethodName(String name) {

        String newName = name.substring(0, 1).toLowerCase() + name.substring(1);
        return newName;
    }

    // This method splits the given name on uppercase letters, ensures the first letter is capitalized, then joins the segments using a space. Ex. 'gameOfThrones' = 'Game Of Thrones'
    private static String normalizeDisplayName(String name) {
        // Split when the next letter is uppercase
        String[] upperCaseSegments = name.split("(?=\\p{Upper})");

        return Arrays.stream(upperCaseSegments).map(
                (upperCaseSegment) -> upperCaseSegment.substring(0, 1).toUpperCase() + upperCaseSegment.substring(1)).collect(Collectors.joining(" "));
    }

    // This class holds references to objects in order to programmatically make calls to Faker objects to generate random data
    protected static class FakerMethodHolder {
        private final String propertyName;
        private final Object methodObject;
        private final Method method;

        public FakerMethodHolder(final String propertyName, final Object methodObject, final Method method) {
            this.propertyName = propertyName;
            this.methodObject = methodObject;
            this.method = method;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public Object getMethodObject() {
            return methodObject;
        }

        public Method getMethod() {
            return method;
        }
    }
}
