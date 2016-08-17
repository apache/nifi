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

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;

/**
 * JDBC / SQL common functions.
 */
public class JdbcCommon {

    private static final int MAX_DIGITS_IN_BIGINT = 19;

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, null);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName)
            throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, recordName, null);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback) throws IOException, SQLException {
        return convertToAvroStream(rs, outStream, recordName, callback, 0);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, final int maxRows)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, recordName);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final ResultSetMetaData meta = rs.getMetaData();
            final int nrOfColumns = meta.getColumnCount();
            long nrOfRows = 0;
            while (rs.next()) {
                if (callback != null) {
                    callback.processRow(rs);
                }
                for (int i = 1; i <= nrOfColumns; i++) {
                    final int javaSqlType = meta.getColumnType(i);
                    final Object value = rs.getObject(i);

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == ARRAY || javaSqlType == BLOB || javaSqlType == CLOB) {
                        // bytes requires little bit different handling
                        byte[] bytes = rs.getBytes(i);
                        ByteBuffer bb = ByteBuffer.wrap(bytes);
                        rec.put(i - 1, bb);

                    } else if (value instanceof Byte) {
                        // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
                        // But value is returned by JDBC as java.lang.Byte
                        // (at least H2 JDBC works this way)
                        // direct put to avro record results:
                        // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                        rec.put(i - 1, ((Byte) value).intValue());

                    } else if (value instanceof BigDecimal) {
                        // Avro can't handle BigDecimal as a number - it will throw an AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
                        rec.put(i - 1, value.toString());

                    } else if (value instanceof BigInteger) {
                        // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                        // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
                        // long (and the schema says it will be), so try to get its value as a long.
                        // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
                        // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
                        int precision = meta.getPrecision(i);
                        if (javaSqlType == BIGINT) {
                            if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                                rec.put(i - 1, value.toString());
                            } else {
                                try {
                                    rec.put(i - 1, ((BigInteger) value).longValueExact());
                                } catch (ArithmeticException ae) {
                                    // Since the value won't fit in a long, convert it to a string
                                    rec.put(i - 1, value.toString());
                                }
                            }
                        } else {
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof Number || value instanceof Boolean) {
                        rec.put(i - 1, value);

                    } else {
                        // The different types that we support are numbers (int, long, double, float),
                        // as well as boolean values and Strings. Since Avro doesn't provide
                        // timestamp types, we want to convert those to Strings. So we will cast anything other
                        // than numbers or booleans to strings by using the toString() method.
                        rec.put(i - 1, value.toString());
                    }
                }
                dataFileWriter.append(rec);
                nrOfRows += 1;

                if(maxRows > 0 && nrOfRows == maxRows)
                    break;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs) throws SQLException {
        return createSchema(rs, null);
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs         The result set to convert to Avro
     * @param recordName The a priori record name to use if it cannot be determined from the result set.
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public static Schema createSchema(final ResultSet rs, String recordName) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(recordName) ? "NiFi_ExecuteSQL_Record" : recordName;
        if (nrOfColumns > 0) {
            String tableNameFromMeta = meta.getTableName(1);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i)) {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                    // to strings as necessary
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    } else {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Did not find direct suitable type, need to be clarified!!!!
                case DECIMAL:
                case NUMERIC:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                // Did not find direct suitable type, need to be clarified!!!!
                case DATE:
                case TIME:
                case TIMESTAMP:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                case CLOB:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToAvroStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
    }
}
