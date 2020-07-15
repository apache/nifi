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

import com.beanit.asn1bean.ber.types.BerBoolean;
import com.beanit.asn1bean.ber.types.BerType;
import org.apache.nifi.jasn1.convert.JASN1Converter;
import org.apache.nifi.jasn1.convert.JASN1TypeAndValueConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class BerBooleanConverter implements JASN1TypeAndValueConverter {
    @Override
    public boolean supportsType(Class<?> berType) {
        return BerBoolean.class.isAssignableFrom(berType);
    }

    @Override
    public DataType convertType(Class<?> berType, JASN1Converter converter) {
        return RecordFieldType.BOOLEAN.getDataType();
    }

    @Override
    public boolean supportsValue(BerType value, DataType dataType) {
        return value instanceof BerBoolean;
    }

    @Override
    public Object convertValue(BerType value, DataType dataType, JASN1Converter converter) {
        final BerBoolean berValue = (BerBoolean) value;

        return berValue.value;
    }
}
