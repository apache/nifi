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
package org.apache.nifi.jasn1.convert.converters;

import com.beanit.asn1bean.ber.types.BerType;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.jasn1.convert.JASN1Converter;
import org.apache.nifi.jasn1.convert.JASN1TypeAndValueConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.function.Supplier;

import static org.apache.nifi.jasn1.JASN1Utils.invokeGetter;
import static org.apache.nifi.jasn1.JASN1Utils.toGetterMethod;

public class BerRecordConverter implements JASN1TypeAndValueConverter {
    private final LoadingCache<Class, RecordSchema> schemaCache;

    public BerRecordConverter(LoadingCache<Class, RecordSchema> schemaCache) {
        this.schemaCache = schemaCache;
    }

    @Override
    public boolean supportsType(Class<?> berType) {
        // Needs to be the last to check
        return true;
    }

    @Override
    public DataType convertType(Class<?> berType, JASN1Converter converter) {
        Supplier<RecordSchema> recordSchemaSupplier = () -> schemaCache.get(berType);
        return RecordFieldType.RECORD.getRecordDataType(recordSchemaSupplier);
    }

    @Override
    public boolean supportsValue(BerType value, DataType dataType) {
        return RecordFieldType.RECORD.equals(dataType.getFieldType());
    }

    @Override
    public Object convertValue(BerType berRecord, DataType dataType, JASN1Converter converter) {
        final Class modelClass = berRecord.getClass();
        final RecordSchema recordSchema = schemaCache.get(modelClass);
        final MapRecord record = new MapRecord(recordSchema, new HashMap<>());

        for (RecordField field : recordSchema.getFields()) {
            String fieldName = field.getFieldName();

            final Object value = invokeGetter(berRecord, toGetterMethod(fieldName));
            if (value instanceof BerType) {
                record.setValue(field, converter.convertValue((BerType) value, field.getDataType()));
            }
        }

        return record;
    }
}
