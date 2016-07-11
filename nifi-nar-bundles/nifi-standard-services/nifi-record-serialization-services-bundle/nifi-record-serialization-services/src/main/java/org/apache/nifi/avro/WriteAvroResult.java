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

package org.apache.nifi.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

public class WriteAvroResult implements RecordSetWriter {
    private final Schema schema;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    public WriteAvroResult(final Schema schema, final String dateFormat, final String timeFormat, final String timestampFormat) {
        this.schema = schema;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);
        this.timestampFormat = new SimpleDateFormat(timestampFormat);
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream outStream) throws IOException {
        Record record = rs.next();
        if (record == null) {
            return WriteResult.of(0, Collections.emptyMap());
        }

        final GenericRecord rec = new GenericData.Record(schema);

        int nrOfRows = 0;
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final RecordSchema recordSchema = rs.getSchema();

            do {
                for (final String fieldName : recordSchema.getFieldNames()) {
                    final Object value = record.getValue(fieldName);

                    final Field field = schema.getField(fieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted;
                    try {
                        converted = convert(value, field.schema(), fieldName);
                    } catch (final SQLException e) {
                        throw new IOException("Failed to write records to stream", e);
                    }

                    rec.put(fieldName, converted);
                }

                dataFileWriter.append(rec);
                nrOfRows++;
            } while ((record = rs.next()) != null);
        }

        return WriteResult.of(nrOfRows, Collections.emptyMap());
    }

    @Override
    public WriteResult write(final Record record, final OutputStream out) throws IOException {
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);
            final RecordSchema recordSchema = record.getSchema();

            for (final String fieldName : recordSchema.getFieldNames()) {
                final Object value = record.getValue(fieldName);

                final Field field = schema.getField(fieldName);
                if (field == null) {
                    continue;
                }

                final Object converted;
                try {
                    converted = convert(value, field.schema(), fieldName);
                } catch (final SQLException e) {
                    throw new IOException("Failed to write records to stream", e);
                }

                rec.put(fieldName, converted);
            }

            dataFileWriter.append(rec);
        }

        return WriteResult.of(1, Collections.emptyMap());
    }


    private Object convert(final Object value, final Schema schema, final String fieldName) throws SQLException, IOException {
        if (value == null) {
            return null;
        }

        // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's maximum portability statement
        if (value instanceof Clob) {
            final Clob clob = (Clob) value;

            long numChars = clob.length();
            char[] buffer = new char[(int) numChars];
            InputStream is = clob.getAsciiStream();
            int index = 0;
            int c = is.read();
            while (c > 0) {
                buffer[index++] = (char) c;
                c = is.read();
            }

            clob.free();
            return new String(buffer);
        }

        if (value instanceof Blob) {
            final Blob blob = (Blob) value;

            final long numChars = blob.length();
            final byte[] buffer = new byte[(int) numChars];
            final InputStream is = blob.getBinaryStream();
            int index = 0;
            int c = is.read();
            while (c > 0) {
                buffer[index++] = (byte) c;
                c = is.read();
            }

            final ByteBuffer bb = ByteBuffer.wrap(buffer);
            blob.free();
            return bb;
        }

        if (value instanceof byte[]) {
            // bytes requires little bit different handling
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof Byte) {
            // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
            // But value is returned by JDBC as java.lang.Byte
            // (at least H2 JDBC works this way)
            // direct put to avro record results:
            // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
            return ((Byte) value).intValue();
        } else if (value instanceof Short) {
            //MS SQL returns TINYINT as a Java Short, which Avro doesn't understand.
            return ((Short) value).intValue();
        } else if (value instanceof BigDecimal) {
            // Avro can't handle BigDecimal as a number - it will throw an AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
            return value.toString();
        } else if (value instanceof BigInteger) {
            // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
            // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
            // long (and the schema says it will be), so try to get its value as a long.
            // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
            // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
            final BigInteger bigInt = (BigInteger) value;
            if (bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                return value.toString();
            } else {
                return bigInt.longValue();
            }
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof Map) {
            // TODO: Revisit how we handle a lot of these cases....
            switch (schema.getType()) {
                case MAP:
                    return value;
                case RECORD:
                    final GenericData.Record avroRecord = new GenericData.Record(schema);

                    final Record record = (Record) value;
                    for (final String recordFieldName : record.getSchema().getFieldNames()) {
                        final Object recordFieldValue = record.getValue(recordFieldName);

                        final Field field = schema.getField(recordFieldName);
                        if (field == null) {
                            continue;
                        }

                        final Object converted = convert(recordFieldValue, field.schema(), recordFieldName);
                        avroRecord.put(recordFieldName, converted);
                    }
                    return avroRecord;
            }

            return value.toString();

        } else if (value instanceof List) {
            return value;
        } else if (value instanceof Object[]) {
            final List<Object> list = new ArrayList<>();
            for (final Object o : ((Object[]) value)) {
                final Object converted = convert(o, schema.getElementType(), fieldName);
                list.add(converted);
            }
            return list;
        } else if (value instanceof Number) {
            return value;
        } else if (value instanceof java.util.Date) {
            final java.util.Date date = (java.util.Date) value;
            return dateFormat.format(date);
        } else if (value instanceof java.sql.Date) {
            final java.sql.Date sqlDate = (java.sql.Date) value;
            final java.util.Date date = new java.util.Date(sqlDate.getTime());
            return dateFormat.format(date);
        } else if (value instanceof Time) {
            final Time time = (Time) value;
            final java.util.Date date = new java.util.Date(time.getTime());
            return timeFormat.format(date);
        } else if (value instanceof Timestamp) {
            final Timestamp time = (Timestamp) value;
            final java.util.Date date = new java.util.Date(time.getTime());
            return timestampFormat.format(date);
        }

        // The different types that we support are numbers (int, long, double, float),
        // as well as boolean values and Strings. Since Avro doesn't provide
        // timestamp types, we want to convert those to Strings. So we will cast anything other
        // than numbers or booleans to strings by using the toString() method.
        return value.toString();
    }


    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }


    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }
}
