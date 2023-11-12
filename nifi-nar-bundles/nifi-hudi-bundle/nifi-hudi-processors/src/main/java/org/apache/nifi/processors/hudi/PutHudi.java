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
package org.apache.nifi.processors.hudi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.DFSPropertiesConfiguration.DEFAULT_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING;
import static org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_CLASS_NAME;
import static org.apache.hudi.keygen.KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG;
import static org.apache.hudi.keygen.KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;

@Tags({"hudi", "put", "table", "store", "record"})
@CapabilityDescription("This processor uses the Hudi Java Client to parse and load records into Hudi tables. " +
        "The processor initialize a Hudi Write Client which can be configured through hudi config files and with user defined dynamic fields." +
        "The target Hudi table should already exist and it must have matching schemas with the incoming records, " +
        "which means the Record Reader schema must contain all the Hudi schema fields, every additional field which is not present in the Hudi schema will be ignored. " +
        "To avoid 'small file problem' it is recommended pre-appending a MergeRecord processor.")
@DynamicProperty(
        name = "Hudi configuration property key.",
        value = "Hudi configuration property value.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds a Hudi configuration entry to the data writer client.")
@WritesAttributes({
        @WritesAttribute(attribute = "hudi.record.count", description = "The number of records in the FlowFile.")
})
public class PutHudi extends AbstractHudiProcessor {

    public static final String HUDI_RECORD_COUNT = "hudi.record.count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE_PATH = new PropertyDescriptor.Builder()
            .name("table-path")
            .displayName("Table Path")
            .description("Path to the Hudi table.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            TABLE_PATH,
            HUDI_CONFIGURATION_RESOURCES,
            HADOOP_CONFIGURATION_RESOURCES,
            KERBEROS_USER_SERVICE
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();

        final boolean kerberosUserServiceIsSet = context.getProperty(KERBEROS_USER_SERVICE).isSet();
        final boolean securityEnabled = SecurityUtil.isSecurityEnabled(getConfigurationFromFiles(getConfigLocations(context)));

        if (securityEnabled && !kerberosUserServiceIsSet) {
            problems.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("'hadoop.security.authentication' is set to 'kerberos' in the hadoop configuration files but no KerberosUserService is configured.")
                    .build());
        }

        if (!securityEnabled && kerberosUserServiceIsSet) {
            problems.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("KerberosUserService is configured but 'hadoop.security.authentication' is not set to 'kerberos' in the hadoop configuration files.")
                    .build());
        }

        return problems;
    }

    @Override
    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final long startNanos = System.nanoTime();
        final Configuration hadoopConfig = getConfigurationFromFiles(getConfigLocations(context));
        final ResourceReference hudiConfig = context.getProperty(HUDI_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().asResource();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String tablePath = context.getProperty(TABLE_PATH).getValue();
        final Map<String, String> dynamicProperties = getDynamicProperties(context, flowFile);

        final HoodieTableMetaClient tableClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(hadoopConfig).build();
        final TypedProperties properties = getTypedProperties(hadoopConfig, hudiConfig, tableClient, dynamicProperties);

        int recordCount;
        try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            final TableSchemaResolver schemaResolver = new TableSchemaResolver(tableClient);
            final Schema schema = schemaResolver.getTableAvroSchema();
            final KeyGenerator keyGenerator = HudiKeyGeneratorFactory.createKeyGenerator(properties);
            final HoodieJavaWriteClient<HoodieAvroPayload> client = createWriterClient(hadoopConfig, tablePath, schema, tableClient, properties);

            List<HoodieRecord<HoodieAvroPayload>> writeRecords = new ArrayList<>();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, schema);
                writeRecords.add(new HoodieAvroRecord<>(keyGenerator.getKey(genericRecord), new HoodieAvroPayload(Option.of(genericRecord))));
            }
            recordCount = writeRecords.size();

            final String newCommitTime = client.startCommit();
            client.insert(writeRecords, newCommitTime);
            client.close();
        } catch (Exception e) {
            getLogger().error("Exception occurred while writing hudi records.", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, HUDI_RECORD_COUNT, String.valueOf(recordCount));
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, tablePath, transferMillis);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Initializing a java data writer client
     *
     * @param hadoopConfig hadoop configuration
     * @param tablePath    path to the hudi table
     * @param schema       table schema
     * @param tableClient  table meta client
     * @param properties   data writer properties
     * @return data writer client
     */
    private HoodieJavaWriteClient<HoodieAvroPayload> createWriterClient(Configuration hadoopConfig, String tablePath, Schema schema, HoodieTableMetaClient tableClient, TypedProperties properties) {
        final HoodieJavaEngineContext hoodieJavaEngineContext = new HoodieJavaEngineContext(hadoopConfig);
        final HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withEngineType(EngineType.JAVA)
                .withSchema(schema.toString())
                .forTable(tableClient.getTableConfig().getTableName())
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withProps(properties)
                .build();

        return new HoodieJavaWriteClient<>(hoodieJavaEngineContext, writeConfig);
    }

    /**
     * Constructs writer configuration from hadoop, hudi and user defined properties.
     *
     * @param hadoopConfig       hadoop configuration
     * @param hudiConfig         hudi configuration
     * @param tableClient        table meta client
     * @param overrideProperties user defined configurations through dynamic properties
     * @return constructed configuration
     */
    private TypedProperties getTypedProperties(Configuration hadoopConfig, ResourceReference hudiConfig, HoodieTableMetaClient tableClient, Map<String, String> overrideProperties) {
        final DFSPropertiesConfiguration config = new DFSPropertiesConfiguration(hadoopConfig, DEFAULT_PATH);

        if (hudiConfig != null) {
            config.addPropsFromFile(new Path(hudiConfig.getLocation()));
        }

        TypedProperties properties = config.getProps();
        addPropertiesFromTableConfig(properties, tableClient.getTableConfig().getProps());
        properties.putAll(overrideProperties);

        if (KeyGenUtils.enableAutoGenerateRecordKeys(properties)) {
            properties.put(RECORD_KEY_GEN_PARTITION_ID_CONFIG, new SecureRandom().nextInt());
            properties.put(RECORD_KEY_GEN_INSTANT_TIME_CONFIG, HoodieActiveTimeline.createNewInstantTime());
        }

        return properties;
    }

    /**
     * Parse and set properties from table configuration to data writer configuration.
     *
     * @param writerProperties data writer configuration
     * @param tableProperties  table configuration
     */
    private void addPropertiesFromTableConfig(TypedProperties writerProperties, TypedProperties tableProperties) {
        if (tableProperties.containsKey(RECORDKEY_FIELDS.key())) {
            writerProperties.put(RECORDKEY_FIELD_NAME.key(), tableProperties.get(RECORDKEY_FIELDS.key()));
        }

        if (tableProperties.containsKey(PARTITION_FIELDS.key())) {
            writerProperties.put(PARTITIONPATH_FIELD_NAME.key(), tableProperties.get(PARTITION_FIELDS.key()));
        }

        if (tableProperties.containsKey(HIVE_STYLE_PARTITIONING_ENABLE.key())) {
            writerProperties.put(HIVE_STYLE_PARTITIONING_ENABLE.key(), tableProperties.get(HIVE_STYLE_PARTITIONING_ENABLE.key()));
        }

        if (tableProperties.containsKey(URL_ENCODE_PARTITIONING.key())) {
            writerProperties.put(URL_ENCODE_PARTITIONING.key(), tableProperties.get(URL_ENCODE_PARTITIONING.key()));
        }

        if (tableProperties.containsKey(KEY_GENERATOR_CLASS_NAME.key())) {
            writerProperties.put(KEYGENERATOR_CLASS_NAME.key(),
                    HudiKeyGeneratorFactory.convertToCommonKeyGenerator(String.valueOf(tableProperties.get(KEY_GENERATOR_CLASS_NAME.key()))));
        }
    }

    /**
     * Collects every non-blank dynamic property from the context.
     *
     * @param context  process context
     * @param flowFile FlowFile to evaluate attribute expressions
     * @return Map of dynamic properties
     */
    public static Map<String, String> getDynamicProperties(ProcessContext context, FlowFile flowFile) {
        return context.getProperties().entrySet().stream()
                // filter non-blank dynamic properties
                .filter(e -> e.getKey().isDynamic()
                        && StringUtils.isNotBlank(e.getValue())
                        && StringUtils.isNotBlank(context.getProperty(e.getKey()).evaluateAttributeExpressions(flowFile).getValue())
                )
                // convert to Map keys and evaluated property values
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).evaluateAttributeExpressions(flowFile).getValue()
                ));
    }

}
