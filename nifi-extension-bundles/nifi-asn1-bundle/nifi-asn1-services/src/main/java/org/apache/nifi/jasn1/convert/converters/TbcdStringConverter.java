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

import com.beanit.asn1bean.ber.types.BerOctetString;
import com.beanit.asn1bean.ber.types.BerType;
import org.apache.nifi.jasn1.convert.JASN1Converter;
import org.apache.nifi.jasn1.convert.JASN1TypeAndValueConverter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class TbcdStringConverter implements JASN1TypeAndValueConverter {

    private static final String TBCD_STRING_TYPE = "TBCDSTRING";
    private static final char[] TBCD_SYMBOLS = "0123456789*#abc".toCharArray();
    private static final int FILLER_DECIMAL_CODE = 15;

    @Override
    public boolean supportsType(Class<?> berType) {
        boolean supportsType = BerOctetString.class.isAssignableFrom(berType) && isTbcdString(berType);

        return supportsType;
    }

    @Override
    public DataType convertType(Class<?> berType, JASN1Converter converter) {
        DataType dataType = RecordFieldType.STRING.getDataType();

        return dataType;
    }

    @Override
    public boolean supportsValue(BerType value, DataType dataType) {
        boolean supportsValue = value instanceof BerOctetString && isTbcdString(value.getClass());

        return supportsValue;
    }

    @Override
    public Object convertValue(BerType value, DataType dataType, JASN1Converter converter) {
        final BerOctetString berValue = ((BerOctetString) value);

        byte[] bytes = berValue.value;

        int size = (bytes == null ? 0 : bytes.length);
        StringBuilder resultBuilder = new StringBuilder(2 * size);

        for (int octetIndex = 0; octetIndex < size; ++octetIndex) {
            int octet = bytes[octetIndex];

            int digit2 = (octet >> 4) & 0xF;
            int digit1 = octet & 0xF;

            if (digit1 == FILLER_DECIMAL_CODE) {
                invalidFiller(octetIndex, octet);
            } else if (digit1 > 15) {
                invalidInteger(digit1);
            } else {
                resultBuilder.append(TBCD_SYMBOLS[digit1]);
            }

            if (digit2 == FILLER_DECIMAL_CODE) {
                if (octetIndex != size - 1) {
                    invalidFiller(octetIndex, octet);
                }
            } else if (digit2 > 15) {
                invalidInteger(digit2);
            } else {
                resultBuilder.append(TBCD_SYMBOLS[digit2]);
            }
        }

        return resultBuilder.toString();
    }

    private boolean isTbcdString(Class<?> berType) {
        Class<?> currentType = berType;
        while (currentType != null) {
            if (currentType.getSimpleName().equals(TBCD_STRING_TYPE)) {
                return true;
            }

            currentType = currentType.getSuperclass();
        }

        return false;
    }

    private void invalidFiller(int octetIndex, int octet) {
        throw new NumberFormatException("Illegal filler in octet " + octetIndex + ": " + octet);
    }

    private void invalidInteger(int digit) {
        throw new IllegalArgumentException(
                "Integer should be between 0 - 15 for Telephony Binary Coded Decimal String. Received " + digit);
    }
}
