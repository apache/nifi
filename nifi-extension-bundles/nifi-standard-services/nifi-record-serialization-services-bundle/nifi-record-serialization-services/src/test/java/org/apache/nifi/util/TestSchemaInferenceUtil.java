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
package org.apache.nifi.util;

import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.stream.Stream;

class TestSchemaInferenceUtil {
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private final TimeValueInference timeInference = new TimeValueInference(DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

    @ParameterizedTest
    @MethodSource("data")
    public void testWithoutTimeInference(final String input, final DataType expectedResult) {
        Assertions.assertEquals(expectedResult, SchemaInferenceUtil.getDataType(input));
    }

    @ParameterizedTest
    @MethodSource("dataForTimeInference")
    public void testWithTimeInference(final String input, final DataType expectedResult) {
        Assertions.assertEquals(expectedResult, SchemaInferenceUtil.getDataType(input, timeInference));
    }

    private static Stream<Arguments> data() {
        return Stream.of(
            Arguments.of(null, null),
            Arguments.of("", null),

            Arguments.of("true", RecordFieldType.BOOLEAN.getDataType()),
            Arguments.of("TRUE", RecordFieldType.BOOLEAN.getDataType()),
            Arguments.of("FALSE", RecordFieldType.BOOLEAN.getDataType()),
            Arguments.of("tRUE", RecordFieldType.BOOLEAN.getDataType()),
            Arguments.of("fALSE", RecordFieldType.BOOLEAN.getDataType()),

            Arguments.of(new BigDecimal(Double.toString(Double.MAX_VALUE - 1)).toPlainString() + ".01", RecordFieldType.DOUBLE.getDataType()),

            Arguments.of(String.valueOf(1.1D), RecordFieldType.FLOAT.getDataType()),


            Arguments.of(String.valueOf(1), RecordFieldType.INT.getDataType()),
            Arguments.of(String.valueOf(Long.MAX_VALUE), RecordFieldType.LONG.getDataType()),

            Arguments.of("c", RecordFieldType.STRING.getDataType()),
            Arguments.of("string", RecordFieldType.STRING.getDataType())
        );
    }

    private static Stream<Arguments> dataForTimeInference() {
        return Stream.of(
            Arguments.of("2017-03-19", RecordFieldType.DATE.getDataType(DATE_FORMAT)),
            Arguments.of("2017-3-19", RecordFieldType.STRING.getDataType()),
            Arguments.of("2017-44-55", RecordFieldType.STRING.getDataType()),
            Arguments.of("2017.03.19", RecordFieldType.STRING.getDataType()),

            Arguments.of("12:13:14", RecordFieldType.TIME.getDataType(TIME_FORMAT)),
            Arguments.of("12:13:00", RecordFieldType.TIME.getDataType(TIME_FORMAT)),
            Arguments.of("12:03:00", RecordFieldType.TIME.getDataType(TIME_FORMAT)),
            Arguments.of("25:13:14", RecordFieldType.STRING.getDataType()),
            Arguments.of("25::14", RecordFieldType.STRING.getDataType()),

            Arguments.of("2019-01-07T14:59:27.817Z", RecordFieldType.TIMESTAMP.getDataType(TIMESTAMP_FORMAT)),
            Arguments.of("2019-01-07T14:59:27.Z", RecordFieldType.STRING.getDataType()),
            Arguments.of("2019-01-07T14:59:27Z", RecordFieldType.STRING.getDataType()),
            Arguments.of("2019-01-07T14:59:27.817", RecordFieldType.STRING.getDataType()),
            Arguments.of("2019-01-07 14:59:27.817Z", RecordFieldType.STRING.getDataType())
        );
    }
}