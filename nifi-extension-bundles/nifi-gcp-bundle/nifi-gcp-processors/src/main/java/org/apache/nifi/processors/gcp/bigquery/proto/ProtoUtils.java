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

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema.Type;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
* Util class for protocol buffer messaging
*/
public class ProtoUtils {

   public static DynamicMessage createMessage(Descriptors.Descriptor descriptor, Map<String, Object> valueMap, TableSchema tableSchema) {
       final DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

       for (final Descriptors.FieldDescriptor field : descriptor.getFields()) {
           final String name = field.getName();
           Object value = valueMap.get(name);
           if (value == null) {
               continue;
           }

           switch (field.getType()) {
           case MESSAGE:
               if (field.isRepeated()) {
                   final Collection<Map<String, Object>> valueMaps;
                   if (value instanceof Object[] arrayValue) {
                       valueMaps = Arrays.stream(arrayValue)
                               .map(item -> (Map<String, Object>) item).toList();
                   } else if (value instanceof Map<?, ?> mapValue) {
                       valueMaps = mapValue.entrySet().stream()
                               .map(entry -> Map.of(
                                       "key", entry.getKey(),
                                       "value", entry.getValue()
                               )).toList();
                   } else {
                       valueMaps = (Collection<Map<String, Object>>) value;
                   }
                   valueMaps.forEach(act -> builder.addRepeatedField(field, createMessage(field.getMessageType(), act, tableSchema)));
               } else {
                   builder.setField(field, createMessage(field.getMessageType(), (Map<String, Object>) value, tableSchema));
               }
               break;

           // INT64 with alias INT, SMALLINT, INTEGER, BIGINT, TINYINT, BYTEINT
           case INT64:
               // Integer in the bigquery table schema maps back to INT64 which is considered to be Long on Java side:
               // https://developers.google.com/protocol-buffers/docs/proto3
               if (value instanceof Integer) {
                   value = Long.valueOf((Integer) value);
               }

               setField(value, field, builder);
               break;

           // FLOAT64
           case DOUBLE:
               if (value instanceof Float) {
                   value = Double.valueOf(value.toString());
               }
               setField(value, field, builder);
               break;

           // matches NUMERIC and BIGNUMERIC types in BigQuery
           // BQTableSchemaToProtoDescriptor.class
           case BYTES:
               if (value instanceof Integer) {
                   value = new BigDecimal((int) value);
               } else if (value instanceof Long) {
                   value = new BigDecimal((long) value);
               } else if (value instanceof Float || value instanceof Double) {
                   value = new BigDecimal(value.toString());
               }

               if (value instanceof BigDecimal) {
                   if (tableSchema.getFields(field.getIndex()).getType().equals(Type.BIGNUMERIC)) {
                       value = BigDecimalByteStringEncoder.encodeToBigNumericByteString((BigDecimal) value);
                   } else if (tableSchema.getFields(field.getIndex()).getType().equals(Type.NUMERIC)) {
                       value = BigDecimalByteStringEncoder.encodeToNumericByteString((BigDecimal) value);
                   }
               }

               setField(value, field, builder);
               break;

           default:
               setField(value, field, builder);
               break;
           }
       }

       return builder.build();
   }

   private static void setField(final Object value, final Descriptors.FieldDescriptor field, final DynamicMessage.Builder builder) {
       if (field.isRepeated()) {
           Collection collection = value.getClass().isArray() ? Arrays.asList((Object[]) value) : (Collection) value;
           collection.forEach(act -> builder.addRepeatedField(field, act));
       } else {
           builder.setField(field, value);
       }
   }
}