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
package org.apache.nifi.apicurio.schemaregistry.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.util.OptionalInt;

public class SchemaUtils {

    private static final ObjectMapper OBJECT_MAPPER = setObjectMapper();

    private SchemaUtils() {
    }

    public static RecordSchema createRecordSchema(final InputStream schemaStream, final String artifactId, final OptionalInt version) throws SchemaNotFoundException, IOException {
        final JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaStream);
        final String schemaText = schemaNode.toString();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier.Builder schemaIdBuilder = SchemaIdentifier.builder()
                    .name(artifactId);

            if (version.isPresent()) {
                schemaIdBuilder.version(version.getAsInt());
            }

            final SchemaIdentifier schemaId = schemaIdBuilder.build();
            return AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
        } catch (final SchemaParseException e) {
            final String errorMessage = String.format("Obtained Schema with name [%s] from Apicurio Schema Registry but the Schema Text " +
                    "that was returned is not a valid Avro Schema", artifactId);
            throw new SchemaNotFoundException(errorMessage, e);
        }
    }


    static ObjectMapper setObjectMapper() {
        return new ObjectMapper();
    }
}
