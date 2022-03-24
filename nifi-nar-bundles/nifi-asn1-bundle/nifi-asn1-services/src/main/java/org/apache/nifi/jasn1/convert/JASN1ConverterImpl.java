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
package org.apache.nifi.jasn1.convert;

import com.beanit.asn1bean.ber.types.BerType;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.jasn1.convert.converters.BerArrayConverter;
import org.apache.nifi.jasn1.convert.converters.BerBitStringConverter;
import org.apache.nifi.jasn1.convert.converters.BerBooleanConverter;
import org.apache.nifi.jasn1.convert.converters.BerDateConverter;
import org.apache.nifi.jasn1.convert.converters.BerDateTimeConverter;
import org.apache.nifi.jasn1.convert.converters.BerEnumConverter;
import org.apache.nifi.jasn1.convert.converters.BerIntegerConverter;
import org.apache.nifi.jasn1.convert.converters.BerOctetStringConverter;
import org.apache.nifi.jasn1.convert.converters.BerRealConverter;
import org.apache.nifi.jasn1.convert.converters.BerRecordConverter;
import org.apache.nifi.jasn1.convert.converters.BerStringConverter;
import org.apache.nifi.jasn1.convert.converters.BerTimeOfDayConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Arrays;
import java.util.List;

public class JASN1ConverterImpl implements JASN1Converter {
    private final List<JASN1TypeAndValueConverter> jasn1TypeAndValueConverters;

    public JASN1ConverterImpl(LoadingCache<Class, RecordSchema> schemaProvider) {
        jasn1TypeAndValueConverters = Arrays.asList(
                new BerBooleanConverter(),
                new BerEnumConverter(),
                new BerIntegerConverter(),
                new BerBitStringConverter(),
                new BerDateConverter(),
                new BerTimeOfDayConverter(),
                new BerDateTimeConverter(),
                new BerRealConverter(),
                new BerStringConverter(),
                new BerOctetStringConverter(),
                new BerArrayConverter(),
                new BerRecordConverter(schemaProvider)
        );
    }

    @Override
    public Object convertValue(BerType value, DataType dataType) {
        Object converted = null;

        for (JASN1TypeAndValueConverter jasn1ValueConverter : jasn1TypeAndValueConverters) {
            if (jasn1ValueConverter.supportsValue(value, dataType)) {
                converted = jasn1ValueConverter.convertValue(value, dataType, this);
                break;
            }
        }

        return converted;
    }

    @Override
    public DataType convertType(Class<?> type) {
        DataType converted = null;

        for (JASN1TypeAndValueConverter jasn1TypeAndValueConverter : jasn1TypeAndValueConverters) {
            if (jasn1TypeAndValueConverter.supportsType(type)) {
                converted = jasn1TypeAndValueConverter.convertType(type, this);
                break;
            }
        }

        return converted;
    }
}
