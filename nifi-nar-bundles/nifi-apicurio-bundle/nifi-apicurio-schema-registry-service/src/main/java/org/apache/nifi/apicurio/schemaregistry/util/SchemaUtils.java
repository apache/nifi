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
import java.io.UncheckedIOException;

public class SchemaUtils {
    private static final ObjectMapper OBJECT_MAPPER = setObjectMapper();

    private SchemaUtils() {
    }

    public static RecordSchema createRecordSchema(final InputStream schemaStream, final String name, final int version) throws SchemaNotFoundException, IOException {
        final JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaStream);
        final String schemaText = schemaNode.toString();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier schemaId = SchemaIdentifier.builder()
                    .name(name)
                    .version(version)
                    .build();
            return AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
        } catch (final SchemaParseException spe) {
            throw new SchemaNotFoundException("Obtained Schema with name " + name
                    + " from Apicurio Schema Registry but the Schema Text that was returned is not a valid Avro Schema");
        }
    }

    public static int getVersionAttribute(InputStream in) {
        final JsonNode metadataNode;
        try {
            metadataNode = OBJECT_MAPPER.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read HTTP response input stream", e);
        }
        return Integer.parseInt(metadataNode.get("version").asText());
    }

    public static ResultAttributes getResultAttributes(InputStream in) {
        final JsonNode jsonNode;
        try {
            jsonNode = OBJECT_MAPPER.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read HTTP response input stream", e);
        }
        final JsonNode artifactNode = jsonNode.get("artifacts").get(0);
        final String groupId = artifactNode.get("groupId").asText();
        final String artifactId = artifactNode.get("id").asText();
        final String name = artifactNode.get("name").asText();
        return new ResultAttributes(groupId, artifactId, name);
    }

    public record ResultAttributes(String groupId, String artifactId, String name) {
    }

    static ObjectMapper setObjectMapper() {
        return new ObjectMapper();
    }
}
