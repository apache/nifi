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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.record.Record;

import java.util.Optional;

/**
 * Collection of implementation of message key resolvers.
 *
 * Message key resolvers specify how to obtain a Kafka message key for a given message.
 */
final public class MessageKeyResolvers {

    private MessageKeyResolvers() {
    }

    /**
     * {@link MessageKeyResolver} that provides the previously existing functionality of treating the value of
     * the message key field as the name of a property in the (flat) value record.
     * The value of this property is extracted and returned as a String.
     * <P/>
     * <B>Note:</B> Nested properties are not supported!
     */
    public static MessageKeyResolver MESSAGE_VALUE_PROPERTY_RESOLVER = (flowFile, record, messageKeyField) ->
            record.getAsString(messageKeyField.getValue());

    /**
     * {@link MessageKeyResolver} that treats the value of the message key field as a {@link RecordPath} into
     * the value record. The value of this property is extracted and returned as a String.
     * <P/>
     * <pre>
     * Example:
     *  message key field config value = '/address/zip'
     *  value record =
     *    {
     *        "id": "10",
     *        "address": {
     *            "zip": "8804"
     *        }
     *    }
     *  resulting message key = "8804"
     *  </pre>
     */
    public static MessageKeyResolver RECORD_PATH_RESOLVER_FACTORY(RecordPathCache recordPathCache) {
        return (FlowFile flowFile, Record record, PropertyValue messageKeyField) -> {
            final RecordPathResult result = recordPathCache.getCompiled(messageKeyField.getValue()).evaluate(record);
            return result.getSelectedFields().findFirst().flatMap( v -> Optional.ofNullable(v.getValue()))
                    .map(Object::toString).orElse(null);
        };
    }

    /**
     * {@link MessageKeyResolver} that treats the value of the message key field as message key to use. It could refer to
     * existing flow file attributes, e.g. {@code ${kafka.key}} if processing and publishing a record that has been
     * consumed from Kafka.
     */
    public static MessageKeyResolver SIMPLE_VALUE_RESOLVER = (flowFile, record, messageKeyField) -> messageKeyField.getValue();
}
