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

package org.apache.nifi.avro;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"avro", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses Avro data and returns each Avro record as an separate Record object. The Avro data may contain the schema itself, "
    + "or the schema can be externalized and accessed by one of the methods offered by the 'Schema Access Strategy' property.")
public class AvroReader extends SchemaRegistryService implements RecordReaderFactory {
    private final AllowableValue EMBEDDED_AVRO_SCHEMA = new AllowableValue("embedded-avro-schema",
        "Use Embedded Avro Schema", "The FlowFile has the Avro Schema embedded within the content, and this schema will be used.");
    private static final int MAX_AVRO_SCHEMA_CACHE_SIZE = 20;

    private final Map<String, Schema> compiledAvroSchemaCache = new LinkedHashMap<String, Schema>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Schema> eldest) {
            return size() >= MAX_AVRO_SCHEMA_CACHE_SIZE;
        }
    };


    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(EMBEDDED_AVRO_SCHEMA);
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(String strategy, SchemaRegistry schemaRegistry, ConfigurationContext context) {
        if (EMBEDDED_AVRO_SCHEMA.getValue().equals(strategy)) {
            return new EmbeddedAvroSchemaAccessStrategy();
        } else {
            return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
        }
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(String allowableValue, SchemaRegistry schemaRegistry, ValidationContext context) {
        if (EMBEDDED_AVRO_SCHEMA.getValue().equals(allowableValue)) {
            return new EmbeddedAvroSchemaAccessStrategy();
        } else {
            return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
        }
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        final String schemaAccessStrategy = getConfigurationContext().getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        if (EMBEDDED_AVRO_SCHEMA.getValue().equals(schemaAccessStrategy)) {
            return new AvroReaderWithEmbeddedSchema(in);
        } else {
            final RecordSchema recordSchema = getSchema(variables, in, null);

            final Schema avroSchema;
            try {
                if (recordSchema.getSchemaFormat().isPresent() & recordSchema.getSchemaFormat().get().equals(AvroTypeUtil.AVRO_SCHEMA_FORMAT)) {
                    final Optional<String> textOption = recordSchema.getSchemaText();
                    if (textOption.isPresent()) {
                        avroSchema = compileAvroSchema(textOption.get());
                    } else {
                        avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                    }
                } else {
                    avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                }
            } catch (final Exception e) {
                throw new SchemaNotFoundException("Failed to compile Avro Schema", e);
            }

            return new AvroReaderWithExplicitSchema(in, recordSchema, avroSchema);
        }
    }

    private Schema compileAvroSchema(final String text) {
        // Access to the LinkedHashMap must be done while synchronized on this.
        // However, if no compiled schema exists, we don't want to remain synchronized
        // while we compile it, as compilation can be expensive. As a result, if there is
        // not a compiled schema already, we will compile it outside of the synchronized
        // block, and then re-synchronize to update the map. All of this is functionally
        // equivalent to calling compiledAvroSchema.computeIfAbsent(text, t -> new Schema.Parser().parse(t));
        // but does so without synchronizing when not necessary.
        Schema compiled;
        synchronized (this) {
            compiled = compiledAvroSchemaCache.get(text);
        }

        if (compiled != null) {
            return compiled;
        }

        final Schema newlyCompiled = new Schema.Parser().parse(text);
        synchronized (this) {
            return compiledAvroSchemaCache.computeIfAbsent(text, t -> newlyCompiled);
        }
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return EMBEDDED_AVRO_SCHEMA;
    }
}
