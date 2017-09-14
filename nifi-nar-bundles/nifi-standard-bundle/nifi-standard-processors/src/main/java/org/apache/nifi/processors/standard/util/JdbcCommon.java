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
import static java.sql.Types.NCLOB;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.function.Function;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * JDBC / SQL common functions.
 */
public class JdbcCommon {

    private static final int MAX_DIGITS_IN_BIGINT = 19;
    private static final int MAX_DIGITS_IN_INT = 9;
    // Derived from MySQL default precision.
    private static final int DEFAULT_PRECISION_VALUE = 10;
    private static final int DEFAULT_SCALE_VALUE = 0;

    public static final String MIME_TYPE_AVRO_BINARY = "application/avro-binary";

    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("dbf-normalize")
            .displayName("Normalize Table/Column Names")
            .description("Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
                    + "will be changed to underscores in order to build a valid Avro record.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("dbf-user-logical-types")
            .displayName("Use Avro Logical Types")
            .description("Whether to use Avro Logical Types for DECIMAL/NUMBER, DATE, TIME and TIMESTAMP columns. "
                    + "If disabled, written as string. "
                    + "If enabled, Logical types are used and written as its underlying type, specifically, "
                    + "DECIMAL/NUMBER as logical 'decimal': written as bytes with additional precision and scale meta data, "
                    + "DATE as logical 'date-millis': written as int denoting days since Unix epoch (1970-01-01), "
                    + "TIME as logical 'time-millis': written as int denoting milliseconds since Unix epoch, "
                    + "and TIMESTAMP as logical 'timestamp-millis': written as long denoting milliseconds since Unix epoch. "
                    + "If a reader of written Avro records also knows these logical types, then these values can be deserialized with more context depending on reader implementation.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
            .name("dbf-default-precision")
            .displayName("Default Decimal Precision")
            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
                    + " a specific 'precision' denoting number of available digits is required."
                    + " Generally, precision is defined by column data type definition or database engines default."
                    + " However undefined precision (0) can be returned from some database engines."
                    + " 'Default Decimal Precision' is used when writing those undefined precision numbers.")
            .defaultValue(String.valueOf(DEFAULT_PRECISION_VALUE))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
            .name("dbf-default-scale")
            .displayName("Default Decimal Scale")
            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
                    + " a specific 'scale' denoting number of available decimal digits is required."
                    + " Generally, scale is defined by column data type definition or database engines default."
                    + " However when undefined precision (0) is returned, scale can also be uncertain with some database engines."
                    + " 'Default Decimal Scale' is used when writing those undefined numbers."
                    + " If a value has more decimals than specified scale, then the value will be rounded-up,"
                    + " e.g. 1.53 becomes 2 with scale 0, and 1.5 with scale 1.")
            .defaultValue(String.valueOf(DEFAULT_SCALE_VALUE))
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();


    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, boolean convertNames) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, null, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, boolean convertNames)
            throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, recordName, null, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, boolean convertNames)
            throws IOException, SQLException {
        return convertToAvroStream(rs, outStream, recordName, callback, 0, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, final int maxRows, boolean convertNames)
            throws SQLException, IOException {
        final AvroConversionOptions options = AvroConversionOptions.builder()
                .recordName(recordName)
                .maxRows(maxRows)
                .convertNames(convertNames)
                .useLogicalTypes(false).build();
        return convertToAvroStream(rs, outStream, options, callback);
    }

    public static class AvroConversionOptions {
        private final String recordName;
        private final int maxRows;
        private final boolean convertNames;
        private final boolean useLogicalTypes;
        private final int defaultPrecision;
        private final int defaultScale;

        private AvroConversionOptions(String recordName, int maxRows, boolean convertNames, boolean useLogicalTypes, int defaultPrecision, int defaultScale) {
            this.recordName = recordName;
            this.maxRows = maxRows;
            this.convertNames = convertNames;
            this.useLogicalTypes = useLogicalTypes;
            this.defaultPrecision = defaultPrecision;
            this.defaultScale = defaultScale;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String recordName;
            private int maxRows = 0;
            private boolean convertNames = false;
            private boolean useLogicalTypes = false;
            private int defaultPrecision = DEFAULT_PRECISION_VALUE;
            private int defaultScale = DEFAULT_SCALE_VALUE;

            /**
             * Specify a priori record name to use if it cannot be determined from the result set.
             */
            public Builder recordName(String recordName) {
                this.recordName = recordName;
                return this;
            }

            public Builder maxRows(int maxRows) {
                this.maxRows = maxRows;
                return this;
            }

            public Builder convertNames(boolean convertNames) {
                this.convertNames = convertNames;
                return this;
            }

            public Builder useLogicalTypes(boolean useLogicalTypes) {
                this.useLogicalTypes = useLogicalTypes;
                return this;
            }

            public Builder defaultPrecision(int defaultPrecision) {
                this.defaultPrecision = defaultPrecision;
                return this;
            }

            public Builder defaultScale(int defaultScale) {
                this.defaultScale = defaultScale;
                return this;
            }

            public AvroConversionOptions build() {
                return new AvroConversionOptions(recordName, maxRows, convertNames, useLogicalTypes, defaultPrecision, defaultScale);
            }
        }
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, final AvroConversionOptions options, final ResultSetRowCallback callback)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, options);
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
                    final Schema fieldSchema = schema.getFields().get(i - 1).schema();

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's maximum portability statement
                    if (javaSqlType == CLOB) {
                        Clob clob = rs.getClob(i);
                        if (clob != null) {
                            long numChars = clob.length();
                            char[] buffer = new char[(int) numChars];
                            InputStream is = clob.getAsciiStream();
                            int index = 0;
                            int c = is.read();
                            while (c > 0) {
                                buffer[index++] = (char) c;
                                c = is.read();
                            }
                            rec.put(i - 1, new String(buffer));
                            clob.free();
                        } else {
                            rec.put(i - 1, null);
                        }
                        continue;
                    }

                    if (javaSqlType == NCLOB) {
                        NClob nClob = rs.getNClob(i);
                        if (nClob != null) {
                            final Reader characterStream = nClob.getCharacterStream();
                            long numChars = (int) nClob.length();
                            final CharBuffer buffer = CharBuffer.allocate((int) numChars);
                            characterStream.read(buffer);
                            buffer.flip();
                            rec.put(i - 1, buffer.toString());
                            nClob.free();
                        } else {
                            rec.put(i - 1, null);
                        }
                        continue;
                    }

                    if (javaSqlType == BLOB) {
                        Blob blob = rs.getBlob(i);
                        if (blob != null) {
                            long numChars = blob.length();
                            byte[] buffer = new byte[(int) numChars];
                            InputStream is = blob.getBinaryStream();
                            int index = 0;
                            int c = is.read();
                            while (c > 0) {
                                buffer[index++] = (byte) c;
                                c = is.read();
                            }
                            ByteBuffer bb = ByteBuffer.wrap(buffer);
                            rec.put(i - 1, bb);
                            blob.free();
                        } else {
                            rec.put(i - 1, null);
                        }
                        continue;
                    }

                    final Object value = rs.getObject(i);

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == ARRAY) {
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
                    } else if(value instanceof Short) {
                        //MS SQL returns TINYINT as a Java Short, which Avro doesn't understand.
                        rec.put(i - 1, ((Short) value).intValue());
                    } else if (value instanceof BigDecimal) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof BigInteger) {
                        // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                        // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
                        // long (and the schema says it will be), so try to get its value as a long.
                        // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
                        // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
                        if (javaSqlType == BIGINT) {
                            int precision = meta.getPrecision(i);
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
                        if (javaSqlType == BIGINT) {
                            int precision = meta.getPrecision(i);
                            if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                                rec.put(i - 1, value.toString());
                            } else {
                                rec.put(i - 1, value);
                            }
                        } else {
                            rec.put(i - 1, value);
                        }

                    } else if (value instanceof Date) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }

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

                if (options.maxRows > 0 && nrOfRows == options.maxRows)
                    break;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs) throws SQLException {
        return createSchema(rs, null, false);
    }

    public static Schema createSchema(final ResultSet rs, String recordName, boolean convertNames) throws SQLException {
        final AvroConversionOptions options = AvroConversionOptions.builder().recordName(recordName).convertNames(convertNames).build();
        return createSchema(rs, options);
    }

    private static void addNullableField(
            FieldAssembler<Schema> builder,
            String columnName,
            Function<BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>>, UnionAccumulator<NullDefault<Schema>>> func
    ) {
        final BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>> and = builder.name(columnName).type().unionOf().nullBuilder().endNull().and();
        func.apply(and).endUnion().noDefault();
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs The result set to convert to Avro
     * @param options Specify various options
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public static Schema createSchema(final ResultSet rs, AvroConversionOptions options) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(options.recordName) ? "NiFi_ExecuteSQL_Record" : options.recordName;
        if (nrOfColumns > 0) {
            String tableNameFromMeta = meta.getTableName(1);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        if (options.convertNames) {
            tableName = normalizeNameForAvro(tableName);
        }

        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
        /**
        *   as per jdbc 4 specs, getColumnLabel will have the alias for the column, if not it will have the column name.
        *  so it may be a better option to check for columnlabel first and if in case it is null is someimplementation,
        *  check for alias. Postgres is the one that has the null column names for calculated fields.
        */
            String nameOrLabel = StringUtils.isNotEmpty(meta.getColumnLabel(i)) ? meta.getColumnLabel(i) :meta.getColumnName(i);
            String columnName = options.convertNames ? normalizeNameForAvro(nameOrLabel) : nameOrLabel;
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) <= MAX_DIGITS_IN_INT)) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                    // to strings as necessary
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Since Avro 1.8, LogicalType is supported.
                case DECIMAL:
                case NUMERIC:
                    if (options.useLogicalTypes) {
                        final int decimalPrecision;
                        final int decimalScale;
                        if (meta.getPrecision(i) > 0) {
                            // When database returns a certain precision, we can rely on that.
                            decimalPrecision = meta.getPrecision(i);
                            decimalScale = meta.getScale(i);
                        } else {
                            // If not, use default precision.
                            decimalPrecision = options.defaultPrecision;
                            // Oracle returns precision=0, scale=-127 for variable scale value such as ROWNUM or function result.
                            // Specifying 'oracle.jdbc.J2EE13Compliant' SystemProperty makes it to return scale=0 instead.
                            // Queries for example, 'SELECT 1.23 as v from DUAL' can be problematic because it can't be mapped with decimal with scale=0.
                            // Default scale is used to preserve decimals in such case.
                            decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : options.defaultScale;
                        }
                        final LogicalTypes.Decimal decimal = LogicalTypes.decimal(decimalPrecision, decimalScale);
                        addNullableField(builder, columnName,
                                u -> u.type(decimal.addToSchema(SchemaBuilder.builder().bytesType())));
                    } else {
                        addNullableField(builder, columnName, u -> u.stringType());
                    }
                    break;

                case DATE:

                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
                                    : u.stringType());
                    break;

                case TIME:
                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()))
                                    : u.stringType());
                    break;

                case TIMESTAMP:
                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()))
                                    : u.stringType());
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " / " + meta.getColumnTypeName(i)
                            + " (table: " + tableName + ", column: " + columnName + ") cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
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
