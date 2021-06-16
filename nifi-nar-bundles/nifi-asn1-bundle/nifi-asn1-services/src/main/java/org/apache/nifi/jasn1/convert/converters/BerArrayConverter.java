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
import org.apache.nifi.jasn1.JASN1Utils;
import org.apache.nifi.jasn1.convert.JASN1Converter;
import org.apache.nifi.jasn1.convert.JASN1TypeAndValueConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;

import java.lang.reflect.Field;
import java.util.List;

import static org.apache.nifi.jasn1.JASN1Utils.getSeqOfElementType;
import static org.apache.nifi.jasn1.JASN1Utils.invokeGetter;
import static org.apache.nifi.jasn1.JASN1Utils.toGetterMethod;

public class BerArrayConverter implements JASN1TypeAndValueConverter {
    @Override
    public boolean supportsType(Class<?> berType) {
        return JASN1Utils.getSeqOfField(berType) != null;
    }

    @Override
    public DataType convertType(Class<?> berType, JASN1Converter converter) {
        Field seqOfField = JASN1Utils.getSeqOfField(berType);
        final Class seqOf = JASN1Utils.getSeqOfElementType(seqOfField);
        return RecordFieldType.ARRAY.getArrayDataType(converter.convertType(seqOf));
    }

    @Override
    public boolean supportsValue(BerType value, DataType dataType) {
        return RecordFieldType.ARRAY.equals(dataType.getFieldType());
    }

    @Override
    public Object convertValue(BerType value, DataType dataType, JASN1Converter converter) {
        // If the field is declared with a direct SEQUENCE OF, then this value is a Parent$Children innerclass,
        // in such a case, use the parent instance to get the seqOfContainer.
        // Otherwise, the value is a separated class holding only seqOf field.

        try {
            // Use the generic type of seqOf field to determine the getter method name.
            final Field seqOfField = value.getClass().getDeclaredField("seqOf");

            final Class seqOf = getSeqOfElementType(seqOfField);
            final String getterMethod = toGetterMethod(seqOf.getSimpleName());

            final DataType elementType = ((ArrayDataType) dataType).getElementType();
            return ((List<? extends BerType>) invokeGetter(value, getterMethod)).stream()
                    .map(fieldValue -> converter.convertValue(fieldValue, elementType)).toArray();
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(value + " doesn't have the expected 'seqOf' field.");
        }
    }
}
