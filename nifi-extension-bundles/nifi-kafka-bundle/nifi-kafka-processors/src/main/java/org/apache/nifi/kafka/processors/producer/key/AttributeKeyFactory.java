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
package org.apache.nifi.kafka.processors.producer.key;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.serialization.record.Record;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;

public class AttributeKeyFactory implements KeyFactory {
    private final FlowFile flowFile;
    private final PropertyValue keyAttribute;
    private final KeyEncoding keyEncoding;

    public AttributeKeyFactory(final FlowFile flowFile, final PropertyValue keyAttribute, final KeyEncoding keyEncoding) {
        this.flowFile = flowFile;
        this.keyAttribute = keyAttribute;
        this.keyEncoding = Objects.requireNonNull(keyEncoding, "Key Encoding required");
    }

    @Override
    public byte[] getKey(final Map<String, String> attributes, final Record record) {
        final String keyAttributeValue = keyAttribute.isSet()
                ? keyAttribute.evaluateAttributeExpressions(flowFile).getValue()
                : attributes.get(KafkaFlowFileAttribute.KAFKA_KEY);
        if (keyAttributeValue == null) {
            return null;
        }
        return switch (keyEncoding) {
            case HEX -> HexFormat.of().parseHex(keyAttributeValue);
            case UTF8 -> keyAttributeValue.getBytes(StandardCharsets.UTF_8);
            case DO_NOT_ADD -> null;
        };
    }
}
