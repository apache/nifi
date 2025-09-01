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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardMessageName;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.decodeZigZag;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.readVarintFromStream;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.readVarintFromStreamAfterFirstByteConsumed;

@Tags({"confluent", "schema", "registry", "protobuf", "message", "name", "resolver"})
@CapabilityDescription("""
    Resolves Protobuf message names from Confluent Schema Registry wire format by decoding message indexes and looking up the fully qualified name in the schema definition
    For Confluent wire format reference see: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    """)
public class ConfluentProtobufMessageNameResolver extends AbstractControllerService implements MessageNameResolver {

    private static final int MAXIMUM_SUPPORTED_ARRAY_LENGTH = 100;
    private static final int MAXIMUM_CACHE_SIZE = 1000;
    private static final int CACHE_EXPIRE_HOURS = 1;

    private Cache<FindMessageNameArguments, MessageName> messageNameCache;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        messageNameCache = Caffeine.newBuilder().maximumSize(MAXIMUM_CACHE_SIZE).expireAfterWrite(Duration.ofHours(CACHE_EXPIRE_HOURS)).build();
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        if (messageNameCache != null) {
            messageNameCache.invalidateAll();
            messageNameCache = null;
        }
    }


    @Override
    public MessageName getMessageName(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final InputStream inputStream) throws IOException {
        final ComponentLog logger = getLogger();

        // Read message indexes directly from stream (Confluent wire format)
        final List<Integer> messageIndexes = readMessageIndexesFromStream(inputStream);
        logger.debug("Decoded message indexes: {}", messageIndexes);
        final FindMessageNameArguments findMessageNameArgs = new FindMessageNameArguments(schemaDefinition, messageIndexes);
        return messageNameCache.get(findMessageNameArgs, this::findMessageName);

    }

    /**
     * Reads message indexes directly from the input stream using Confluent wire format.
     * Format: [array_length:varint][index1:varint][index2:varint]...[indexN:varint]
     * Special case: single 0 byte means first message (index 0)
     * <p>
     * <a href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">Wire format</a>
     *
     * @param inputStream the input stream positioned after the Confluent header (magic byte + schema ID)
     * @return list of message indexes
     * @throws IOException if unable to read from stream or invalid format
     */
    private List<Integer> readMessageIndexesFromStream(final InputStream inputStream) throws IOException {
        // Special case: check if first byte is 0 (most common case)
        final int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new IOException("Unexpected end of stream while reading message indexes");
        }

        if (firstByte == 0) {
            // Single 0 byte means first message type (index 0)
            return List.of(0);
        }

        // General case: read array length as varint
        int arrayLength = readVarintFromStreamAfterFirstByteConsumed(inputStream, firstByte);
        arrayLength = decodeZigZag(arrayLength);

        if (arrayLength < 0 || arrayLength > MAXIMUM_SUPPORTED_ARRAY_LENGTH) { // Reasonable limit
            throw new IllegalStateException("Invalid message index array length: " + arrayLength);
        }

        // Read each index as varint
        final List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < arrayLength; i++) {
            final int rawIndex = readVarintFromStream(inputStream);
            final int index = decodeZigZag(rawIndex);
            indexes.add(index);
        }

        return indexes;
    }

    /**
     * Finds the fully qualified message name in the protobuf schema using the message indexes.
     */
    private MessageName findMessageName(final FindMessageNameArguments findMessageNameArguments) {
        try {
            final List<Integer> messageIndexes = findMessageNameArguments.messageIndexes();
            final String schemaText = findMessageNameArguments.schemaDefinition().getText();
            // Parse the protobuf schema using AntlrProtobufMessageSchemaParser
            final AntlrProtobufMessageSchemaParser reader = new AntlrProtobufMessageSchemaParser();
            final List<ProtobufMessageSchema> rootMessages = reader.parse(schemaText);

            if (messageIndexes.isEmpty()) {
                // Return the topmost root message name
                if (!rootMessages.isEmpty()) {
                    return getFullyQualifiedName(singletonList(rootMessages.getFirst()));
                } else {
                    throw new IllegalStateException("No root messages found in schema");
                }
            }

            // Navigate through the message hierarchy using indexes
            ProtobufMessageSchema currentMessage;
            List<ProtobufMessageSchema> currentLevel = rootMessages;
            final List<ProtobufMessageSchema> messagePath = new ArrayList<>();

            for (final int index : messageIndexes) {
                if (index >= currentLevel.size()) {
                    final String msg = format("Message index %d out of bounds for level with %d messages. Message indexes: [%s]", index, currentLevel.size(), messageIndexes);
                    throw new IllegalStateException(msg);
                }

                currentMessage = currentLevel.get(index);
                messagePath.add(currentMessage);

                // Move to nested messages of the current message
                currentLevel = currentMessage.getChildMessageSchemas();
            }

            // Return the fully qualified name including parent message hierarchy
            return getFullyQualifiedName(messagePath);

        } catch (final Exception e) {
            throw new IllegalStateException("Failed to parse protobuf schema", e);
        }
    }

    /**
     * Gets the fully qualified name for a message path, including parent message names.
     */
    private MessageName getFullyQualifiedName(final List<ProtobufMessageSchema> messagePath) {
        final ProtobufMessageSchema firstMessage = messagePath.getFirst();

        final String fullName = messagePath.stream()
            .map(ProtobufMessageSchema::getName)
            .collect(Collectors.joining("."));

        return new StandardMessageName(firstMessage.getPackageName(), fullName);
    }

    private record FindMessageNameArguments(SchemaDefinition schemaDefinition, List<Integer> messageIndexes) {
    }
}


