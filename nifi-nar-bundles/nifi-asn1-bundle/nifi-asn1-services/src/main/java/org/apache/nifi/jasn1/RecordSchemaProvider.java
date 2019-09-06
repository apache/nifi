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
package org.apache.nifi.jasn1;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.jasn1.convert.JASN1ConverterImpl;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RecordSchemaProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RecordSchemaProvider.class);

    private final LoadingCache<Class, RecordSchema> schemaCache = Caffeine.newBuilder()
        .maximumSize(100)
        .build(this::generateRecordSchema);

    public RecordSchema get(Class type) {
        return schemaCache.get(type);
    }

    private RecordSchema generateRecordSchema(Class type) {
        final SimpleRecordSchema schema = createBlankRecordSchema(type);

        List<Class> typeHierarchy = new ArrayList<>();

        Class currentType = type;
        while (!Object.class.equals(currentType)) {
            typeHierarchy.add(currentType);
            currentType = currentType.getSuperclass();
        }

        final List<RecordField> fields = typeHierarchy.stream()
            .map(Class::getDeclaredFields)
            .flatMap(Arrays::stream)
            .map(this::toRecordField)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        schema.setFields(fields);

        return schema;
    }

    private SimpleRecordSchema createBlankRecordSchema(Class type) {
        final SchemaIdentifier schemaId = new StandardSchemaIdentifier.Builder()
            .name(type.getCanonicalName())
            .build();
        final SimpleRecordSchema schema = new SimpleRecordSchema(schemaId);
        schema.setSchemaNamespace(type.getPackage().getName());
        schema.setSchemaName(type.getSimpleName());
        return schema;
    }

    private RecordField toRecordField(Field field) {
        if (!JASN1Utils.isRecordField(field)) {
            return null;
        }

        final Class<?> type = field.getType();

        final DataType fieldType = getDataType(type);

        return new RecordField(field.getName(), fieldType, true);
    }

    private DataType getDataType(Class<?> type) {
        return new JASN1ConverterImpl(schemaCache).convertType(type);
    }

    public LoadingCache<Class, RecordSchema> getSchemaCache() {
        return schemaCache;
    }
}
