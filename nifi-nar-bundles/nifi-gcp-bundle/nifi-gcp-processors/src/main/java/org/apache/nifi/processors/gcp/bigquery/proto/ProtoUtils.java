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

package org.apache.nifi.processors.gcp.bigquery.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Util class for protocol buffer messaging
 */
public class ProtoUtils {

    public static DynamicMessage createMessage(Descriptors.Descriptor descriptor, Map<String, Object> valueMap) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            String name = field.getName();
            Object value = valueMap.get(name);
            if (value == null) {
                continue;
            }

            if (Descriptors.FieldDescriptor.Type.MESSAGE.equals(field.getType())) {
                if (field.isRepeated()) {
                    Collection collection = value.getClass().isArray() ? Arrays.asList((Object[]) value) : (Collection) value;
                    collection.forEach(act -> builder.addRepeatedField(field, createMessage(field.getMessageType(), (Map<String, Object>) act)));
                } else {
                    builder.setField(field, createMessage(field.getMessageType(), (Map<String, Object>) value));
                }
            } else {
                // Integer in the bigquery table schema maps back to INT64 which is considered to be Long on Java side:
                // https://developers.google.com/protocol-buffers/docs/proto3
                if (value instanceof Integer && (field.getType() == Descriptors.FieldDescriptor.Type.INT64)) {
                    value = Long.valueOf((Integer) value);
                }

                if (field.isRepeated()) {
                    Collection collection = value.getClass().isArray() ? Arrays.asList((Object[]) value) : (Collection) value;
                    collection.forEach(act -> builder.addRepeatedField(field, act));
                } else {
                    builder.setField(field, value);
                }
            }
        }

        return builder.build();
    }
}
