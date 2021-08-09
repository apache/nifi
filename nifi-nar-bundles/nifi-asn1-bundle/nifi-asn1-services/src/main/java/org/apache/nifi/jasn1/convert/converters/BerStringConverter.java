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
import com.beanit.asn1bean.ber.types.string.BerBMPString;
import com.beanit.asn1bean.ber.types.string.BerGeneralString;
import com.beanit.asn1bean.ber.types.string.BerGraphicString;
import com.beanit.asn1bean.ber.types.string.BerIA5String;
import com.beanit.asn1bean.ber.types.string.BerNumericString;
import com.beanit.asn1bean.ber.types.string.BerPrintableString;
import com.beanit.asn1bean.ber.types.string.BerTeletexString;
import com.beanit.asn1bean.ber.types.string.BerUTF8String;
import com.beanit.asn1bean.ber.types.string.BerVideotexString;
import com.beanit.asn1bean.ber.types.string.BerVisibleString;
import org.apache.nifi.jasn1.convert.JASN1Converter;
import org.apache.nifi.jasn1.convert.JASN1TypeAndValueConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.Arrays;
import java.util.List;

public class BerStringConverter implements JASN1TypeAndValueConverter {
    private static final List<Class<?>> supportedBerTypes = Arrays.asList(
            BerGeneralString.class,
            BerGraphicString.class,
            BerIA5String.class,
            BerNumericString.class,
            BerPrintableString.class,
            BerTeletexString.class,
            BerVisibleString.class,
            BerVideotexString.class,
            BerBMPString.class,
            BerUTF8String.class
    );

    @Override
    public boolean supportsType(Class<?> berType) {
        for (Class<?> supportedBerType : supportedBerTypes) {
            if (supportedBerType.isAssignableFrom(berType)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public DataType convertType(Class<?> berType, JASN1Converter converter) {
        return RecordFieldType.STRING.getDataType();
    }

    @Override
    public boolean supportsValue(BerType value, DataType dataType) {
        for (Class<?> supportedBerType : supportedBerTypes) {
            if (supportedBerType.isInstance(value)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Object convertValue(BerType value, DataType dataType, JASN1Converter converter) {
        return value.toString();
    }
}
