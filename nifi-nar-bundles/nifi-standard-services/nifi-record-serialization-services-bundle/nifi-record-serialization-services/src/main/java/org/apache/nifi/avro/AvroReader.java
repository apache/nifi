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
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;

@Tags({"avro", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses Avro data and returns each Avro record as an separate Record object. The Avro data may contain the schema itself, "
    + "or the schema can be externalized and accessed by one of the methods offered by the 'Schema Access Strategy' property.")
public class AvroReader extends SchemaRegistryService implements RecordReaderFactory {
    private final AllowableValue EMBEDDED_AVRO_SCHEMA = new AllowableValue("embedded-avro-schema",
        "Use Embedded Avro Schema", "The FlowFile has the Avro Schema embedded within the content, and this schema will be used.");


    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(EMBEDDED_AVRO_SCHEMA);
        return allowableValues;
    }

    @Override
    public RecordReader createRecordReader(final FlowFile flowFile, final InputStream in, final ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        final String schemaAccessStrategy = getConfigurationContext().getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        if (EMBEDDED_AVRO_SCHEMA.getValue().equals(schemaAccessStrategy)) {
            return new AvroReaderWithEmbeddedSchema(in);
        } else {
            return new AvroReaderWithExplicitSchema(in, getSchema(flowFile, in));
        }
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return EMBEDDED_AVRO_SCHEMA;
    }
}
