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

import org.apache.nifi.confluent.schema.ProtobufMessageSchema;
import org.apache.nifi.confluent.schema.VarintUtils;
import org.apache.nifi.schemaregistry.services.MessageName;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Computes the Confluent wire format message index path for a target message within a parsed
 * Protobuf schema, and encodes it to bytes. This is the inverse of the message index decoding
 * performed by {@code ConfluentProtobufMessageNameResolver}: given a fully qualified message
 * name, it locates the path of declaration-order indexes leading to that message and encodes it
 * as zigzag varints, applying the single-byte {@code 0x00} optimization for the common case of
 * the first root message.
 * <p>
 * <a href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">See the Confluent protobuf wire format.</a>
 * <p>
 * This class has no dependency on the NiFi framework and can be exercised directly with plain
 * unit tests.
 */
final class ProtobufMessageIndexEncoder {

    private static final byte[] FIRST_ROOT_MESSAGE_INDEX = {0x00};

    private ProtobufMessageIndexEncoder() {
    }

    /**
     * Encodes the message index path for the given message name within the given schema.
     *
     * @param rootMessages the root messages of the parsed Protobuf schema, in declaration order
     * @param messageName  the target message name to locate
     * @return the encoded message index bytes
     * @throws IllegalStateException if the message name cannot be located within the schema
     */
    static byte[] encode(final List<ProtobufMessageSchema> rootMessages, final MessageName messageName) {
        final List<Integer> messageIndexPath = findMessageIndexPath(rootMessages, messageName);

        if (messageIndexPath.size() == 1 && messageIndexPath.getFirst() == 0) {
            return FIRST_ROOT_MESSAGE_INDEX;
        }

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.writeBytes(VarintUtils.writeZigZagVarint(messageIndexPath.size()));
        for (final int index : messageIndexPath) {
            output.writeBytes(VarintUtils.writeZigZagVarint(index));
        }
        return output.toByteArray();
    }

    private static List<Integer> findMessageIndexPath(final List<ProtobufMessageSchema> rootMessages, final MessageName messageName) {
        // Match on the fully qualified name across the message tree rather than on the namespace/name split, so the
        // encoder is robust to how the MessageName was constructed (a statically configured "package.Outer.Nested"
        // and a resolver-produced name both reduce to the same fully qualified name).
        final String targetFullyQualifiedName = messageName.getFullyQualifiedName();

        for (int rootIndex = 0; rootIndex < rootMessages.size(); rootIndex++) {
            final ProtobufMessageSchema rootMessage = rootMessages.get(rootIndex);
            final String rootFullyQualifiedName = rootMessage.getPackageName()
                .map(packageName -> packageName + "." + rootMessage.getName())
                .orElseGet(rootMessage::getName);

            final List<Integer> messageIndexPath = descend(rootMessage, rootFullyQualifiedName, targetFullyQualifiedName);
            if (messageIndexPath != null) {
                messageIndexPath.addFirst(rootIndex);
                return messageIndexPath;
            }
        }

        throw new IllegalStateException(format("Message not found in schema definition: %s", targetFullyQualifiedName));
    }

    private static List<Integer> descend(final ProtobufMessageSchema currentMessage, final String currentFullyQualifiedName, final String targetFullyQualifiedName) {
        if (currentFullyQualifiedName.equals(targetFullyQualifiedName)) {
            return new ArrayList<>();
        }

        final List<ProtobufMessageSchema> children = currentMessage.getChildMessageSchemas();
        for (int childIndex = 0; childIndex < children.size(); childIndex++) {
            final ProtobufMessageSchema child = children.get(childIndex);
            final String childFullyQualifiedName = currentFullyQualifiedName + "." + child.getName();

            final List<Integer> messageIndexPath = descend(child, childFullyQualifiedName, targetFullyQualifiedName);
            if (messageIndexPath != null) {
                messageIndexPath.addFirst(childIndex);
                return messageIndexPath;
            }
        }

        return null;
    }
}
