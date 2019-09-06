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

import com.beanit.asn1bean.ber.types.BerType;
import org.apache.nifi.jasn1.convert.JASN1ConverterImpl;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.apache.nifi.jasn1.JASN1Utils.getSeqOfElementType;
import static org.apache.nifi.jasn1.JASN1Utils.getSeqOfField;
import static org.apache.nifi.jasn1.JASN1Utils.invokeGetter;
import static org.apache.nifi.jasn1.JASN1Utils.toGetterMethod;

public class JASN1RecordReader implements RecordReader {

    private final Class<? extends BerType> rootClass;
    private final Class<? extends BerType> recordModelClass;
    private final Class<? extends RecordModelIteratorProvider> iteratorProviderClass;
    private final String recordField;
    private final Field seqOfField;
    private final RecordSchemaProvider schemaProvider;
    private final ClassLoader classLoader;
    private final InputStream inputStream;
    private final ComponentLog logger;

    private Iterator<BerType> recordModelIterator;

    private <T> T withClassLoader(Supplier<T> supplier) {
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (classLoader != null) {
                Thread.currentThread().setContextClassLoader(classLoader);
            }

            return supplier.get();
        } finally {
            if (classLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public JASN1RecordReader(String rootClassName, String recordField,
                             RecordSchemaProvider schemaProvider, ClassLoader classLoader,
                             String iteratorProviderClassName,
                             InputStream inputStream, ComponentLog logger) {

        this.schemaProvider = schemaProvider;
        this.classLoader = classLoader;
        this.inputStream = inputStream;
        this.logger = logger;

        this.recordField = recordField;
        this.rootClass = withClassLoader(() -> {
            try {
                return (Class<? extends BerType>) classLoader.loadClass(rootClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("The root class " + rootClassName + " was not found.", e);
            }
        });

        this.iteratorProviderClass = withClassLoader(() -> {
            if (StringUtils.isEmpty(iteratorProviderClassName)) {
                return StandardRecordModelIteratorProvider.class;
            }

            try {
                return (Class<? extends RecordModelIteratorProvider>) classLoader.loadClass(iteratorProviderClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("The iterator provider class " + iteratorProviderClassName + " was not found.", e);
            }
        });

        if (StringUtils.isEmpty(recordField)) {
            recordModelClass = rootClass;
            seqOfField = null;
        } else {
            try {
                final Method recordModelGetter = rootClass.getMethod(toGetterMethod(recordField));
                final Class<?> readPointType = recordModelGetter.getReturnType();
                seqOfField = getSeqOfField(readPointType);
                if (seqOfField != null) {
                    recordModelClass = getSeqOfElementType(seqOfField);
                } else {
                    recordModelClass = (Class<? extends BerType>) readPointType;
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to get record model class due to " + e, e);
            }
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {

        return withClassLoader(() -> {
            if (recordModelIterator == null) {

                final RecordModelIteratorProvider recordModelIteratorProvider;
                try {
                    recordModelIteratorProvider = iteratorProviderClass.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to instantiate " + iteratorProviderClass.getCanonicalName(), e);
                }

                recordModelIterator = recordModelIteratorProvider.iterator(inputStream, logger, rootClass, recordField, seqOfField);
            }

            if (recordModelIterator.hasNext()) {
                return convertBerRecord(recordModelIterator.next());
            } else {
                return null;
            }

        });
    }

    @SuppressWarnings("unchecked")
    private Object convertBerValue(final String name, final DataType dataType, final BerType instance, final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BerType) {
            return new JASN1ConverterImpl(schemaProvider.getSchemaCache()).convertValue((BerType)value, dataType);
        } else {
            return null;
        }
    }

    private Record convertBerRecord(BerType berRecord) {
        final Class<? extends BerType> recordClass = berRecord.getClass();
        final RecordSchema recordSchema = schemaProvider.get(recordClass);
        final MapRecord record = new MapRecord(recordSchema, new HashMap<>());

        for (RecordField field : recordSchema.getFields()) {
            String fieldName = field.getFieldName();

            final Object value = invokeGetter(berRecord, toGetterMethod(fieldName));
            record.setValue(field, convertBerValue(fieldName, field.getDataType(), berRecord, value));
        }

        return record;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return withClassLoader(() -> schemaProvider.get(recordModelClass));
    }

    @Override
    public void close() throws IOException {

    }
}
