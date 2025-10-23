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
package org.apache.nifi.parquet;

import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.parquet.record.WriteParquetResult;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"parquet", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("Writes the contents of a RecordSet in Parquet format.")
public class ParquetRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Cache Size")
            .description("Specifies how many Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor INT96_FIELDS = new PropertyDescriptor.Builder()
            .name("INT96 Fields")
            .description("List of fields with full path that should be treated as INT96 timestamps.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    private LoadingCache<String, Schema> compiledAvroSchemaCache;
    private String int96Fields;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        compiledAvroSchemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schemaText -> new Schema.Parser().parse(schemaText));

        if (context.getProperty(INT96_FIELDS).isSet()) {
            int96Fields = context.getProperty(INT96_FIELDS).getValue();
        } else {
            int96Fields = null;
        }
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema recordSchema,
                                        final OutputStream out, final Map<String, String> variables) throws IOException {
        final ParquetConfig parquetConfig = createParquetConfig(getConfigurationContext(), variables);
        parquetConfig.setInt96Fields(int96Fields);

        try {
            final Schema avroSchema;
            try {
                if (recordSchema.getSchemaFormat().isPresent() && recordSchema.getSchemaFormat().get().equals(AvroTypeUtil.AVRO_SCHEMA_FORMAT)) {
                    final Optional<String> textOption = recordSchema.getSchemaText();
                    if (textOption.isPresent()) {
                        avroSchema = compiledAvroSchemaCache.get(textOption.get());
                    } else {
                        avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                    }
                } else {
                    avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                }
            } catch (final Exception e) {
                throw new SchemaNotFoundException("Failed to compile Avro Schema", e);
            }

            // These attributes should not be carried over to a newly written Parquet RecordSet, because then
            // processors reading FlowFiles with any of these, might try to jump to non-existing locations.
            final Set<String> propertiesToBeCleared = new HashSet<>(Arrays.asList(
                    ParquetAttribute.RECORD_OFFSET,
                    ParquetAttribute.FILE_RANGE_START_OFFSET,
                    ParquetAttribute.FILE_RANGE_END_OFFSET
            ));
            final Map<String, String> filteredVariables = variables.entrySet()
                    .stream()
                    .filter(entry -> !propertiesToBeCleared.contains(entry.getKey()))
                    .collect(toMap(Entry::getKey, Entry::getValue));

            return new WriteParquetResult(avroSchema, recordSchema, getSchemaAccessWriter(recordSchema, filteredVariables), out, parquetConfig, logger);

        } catch (final SchemaNotFoundException e) {
            throw new ProcessException("Could not determine the Avro Schema to use for writing the content", e);
        }
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("cache-size", CACHE_SIZE.getName());
        config.renameProperty("int96-fields", INT96_FIELDS.getName());
        config.renameProperty(ParquetUtils.OLD_ROW_GROUP_SIZE_PROPERTY_NAME, ParquetUtils.ROW_GROUP_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_PAGE_SIZE_PROPERTY_NAME, ParquetUtils.PAGE_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_DICTIONARY_PAGE_SIZE_PROPERTY_NAME, ParquetUtils.DICTIONARY_PAGE_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_MAX_PADDING_SIZE_PROPERTY_NAME, ParquetUtils.MAX_PADDING_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_ENABLE_DICTIONARY_ENCODING_PROPERTY_NAME, ParquetUtils.ENABLE_DICTIONARY_ENCODING.getName());
        config.renameProperty(ParquetUtils.OLD_ENABLE_VALIDATION_PROPERTY_NAME, ParquetUtils.ENABLE_VALIDATION.getName());
        config.renameProperty(ParquetUtils.OLD_WRITER_VERSION_PROPERTY_NAME, ParquetUtils.WRITER_VERSION.getName());
        config.renameProperty(ParquetUtils.OLD_AVRO_ADD_LIST_ELEMENT_RECORDS_PROPERTY_NAME, ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS.getName());
        config.renameProperty(ParquetUtils.OLD_AVRO_WRITE_OLD_LIST_STRUCTURE_PROPERTY_NAME, ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE.getName());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CACHE_SIZE);
        properties.add(ParquetUtils.COMPRESSION_TYPE);
        properties.add(ParquetUtils.ROW_GROUP_SIZE);
        properties.add(ParquetUtils.PAGE_SIZE);
        properties.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        properties.add(ParquetUtils.MAX_PADDING_SIZE);
        properties.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        properties.add(ParquetUtils.ENABLE_VALIDATION);
        properties.add(ParquetUtils.WRITER_VERSION);
        properties.add(ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE);
        properties.add(ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS);
        properties.add(INT96_FIELDS);
        return properties;
    }
}
