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
package org.apache.nifi.processors.standard.util;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.BIGINT;
import static org.apache.nifi.processors.standard.util.JdbcCommonTestUtils.convertResultSetToAvroInputStream;
import static org.apache.nifi.processors.standard.util.JdbcCommonTestUtils.resultSetReturningMetadata;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestJdbcCommonConvertToAvro {

    private final static boolean SIGNED = true;
    private final static boolean UNSIGNED = false;

    private static int[] range(int start, int end) {
        return IntStream.rangeClosed(start, end).toArray();
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<TestParams> data() {
        Map<Integer, int[]> typeWithPrecisionRange = new HashMap<>();
        typeWithPrecisionRange.put(TINYINT, range(1,3));
        typeWithPrecisionRange.put(SMALLINT, range(1,5));
        typeWithPrecisionRange.put(INTEGER, range(1,9));

        ArrayList<TestParams> params = new ArrayList<>();

        typeWithPrecisionRange.forEach( (sqlType, precisions) -> {
            for (int precision : precisions) {
                params.add(new TestParams(sqlType, precision, SIGNED));
                params.add(new TestParams(sqlType, precision, UNSIGNED));
            }
        });
        // remove cases that we know should fail
        params.removeIf(param ->
            param.sqlType == INTEGER
                    &&
            param.precision == 9
                    &&
            param.signed == UNSIGNED
        );

        return params;
    }

    @Parameterized.Parameter
    public TestParams testParams;

    static class TestParams {
        int sqlType;
        int precision;
        boolean signed;

        TestParams(int sqlType, int precision, boolean signed) {
            this.sqlType = sqlType;
            this.precision = precision;
            this.signed = signed;
        }
        private String humanReadableType() {
            switch(sqlType){
                case TINYINT:
                    return "TINYINT";
                case INTEGER:
                    return "INTEGER";
                case SMALLINT:
                    return "SMALLINT";
                case BIGINT:
                    return "BIGINT";
                default:
                    return "UNKNOWN - ADD TO LIST";
            }
        }
        private String humanReadableSigned() {
            if(signed) return "SIGNED";
            return "UNSIGNED";
        }
        public String toString(){
            return String.format(
                    "TestParams(SqlType=%s, Precision=%s, Signed=%s)",
                    humanReadableType(),
                    precision,
                    humanReadableSigned());
        }
    }

    @Test
    public void testConvertToAvroStreamForNumbers() throws SQLException, IOException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(testParams.sqlType);
        when(metadata.isSigned(1)).thenReturn(testParams.signed);
        when(metadata.getPrecision(1)).thenReturn(testParams.precision);
        when(metadata.getColumnName(1)).thenReturn("t_int");
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = resultSetReturningMetadata(metadata);

        final int ret = 0;
        when(rs.getObject(Mockito.anyInt())).thenReturn(ret);

        final InputStream instream = convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals(Integer.toString(ret), record.get("t_int").toString());
            }
        }
    }
}
