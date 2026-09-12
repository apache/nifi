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
package org.apache.nifi.confluent.schemaregistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.confluent.schema.AntlrProtobufMessageSchemaParser;
import org.apache.nifi.confluent.schema.ProtobufMessageSchema;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schemaregistry.services.MessageIndexWriter;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@Tags({"confluent", "schema", "registry", "protobuf", "message", "index", "writer"})
@CapabilityDescription("""
    Writes Protobuf message index information in Confluent Schema Registry wire format by encoding the declaration-order path to a given message name within the schema definition.
    For Confluent wire format reference see: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    """)
public class ConfluentProtobufMessageIndexWriter extends AbstractControllerService implements MessageIndexWriter {

    private static final int MAXIMUM_CACHE_SIZE = 1000;
    private static final int CACHE_EXPIRE_HOURS = 1;

    private Cache<EncodeMessageIndexArguments, byte[]> messageIndexCache;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        messageIndexCache = Caffeine.newBuilder().maximumSize(MAXIMUM_CACHE_SIZE).expireAfterWrite(Duration.ofHours(CACHE_EXPIRE_HOURS)).build();
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        if (messageIndexCache != null) {
            messageIndexCache.invalidateAll();
            messageIndexCache = null;
        }
    }

    @Override
    public void writeMessageIndex(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final MessageName messageName, final OutputStream outputStream) throws IOException {
        final EncodeMessageIndexArguments encodeMessageIndexArguments = new EncodeMessageIndexArguments(schemaDefinition, messageName);
        final byte[] encodedMessageIndex = messageIndexCache.get(encodeMessageIndexArguments, this::encodeMessageIndex);
        outputStream.write(encodedMessageIndex);
    }

    private byte[] encodeMessageIndex(final EncodeMessageIndexArguments encodeMessageIndexArguments) {
        try {
            final String schemaText = encodeMessageIndexArguments.schemaDefinition().getText();
            final AntlrProtobufMessageSchemaParser parser = new AntlrProtobufMessageSchemaParser();
            final List<ProtobufMessageSchema> rootMessages = parser.parse(schemaText);
            return ProtobufMessageIndexEncoder.encode(rootMessages, encodeMessageIndexArguments.messageName());
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to parse protobuf schema", e);
        }
    }

    private record EncodeMessageIndexArguments(SchemaDefinition schemaDefinition, MessageName messageName) {
    }
}
