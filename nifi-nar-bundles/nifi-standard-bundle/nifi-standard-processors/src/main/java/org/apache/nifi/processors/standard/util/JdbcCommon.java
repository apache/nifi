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
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import javax.xml.bind.DatatypeConverter;

/**
 * JDBC / SQL common functions.
 */
public class JdbcCommon {

    private static final int MAX_DIGITS_IN_BIGINT = 19;
    private static final int MAX_DIGITS_IN_INT = 9;
    // Derived from MySQL default precision.
    private static final int DEFAULT_PRECISION_VALUE = 10;
    private static final int DEFAULT_SCALE_VALUE = 0;

    public static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");
    public static final Pattern SQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("sql\\.args\\.(\\d+)\\.type");
    public static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static void createEmptyAvroStream(final OutputStream outStream) throws IOException {
        final FieldAssembler<Schema> builder = SchemaBuilder.record("NiFi_ExecuteSQL_Record").namespace("any.data").fields();
        final Schema schema = builder.endRecord();

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);
        }
    }

    public static class AvroConversionOptions {

        private final String recordName;
        private final int maxRows;
        private final boolean convertNames;
        private final boolean useLogicalTypes;
        private final int defaultPrecision;
        private final int defaultScale;
        private final CodecFactory codec;

        private AvroConversionOptions(String recordName, int maxRows, boolean convertNames, boolean useLogicalTypes,
                int defaultPrecision, int defaultScale, CodecFactory codec) {
            this.recordName = recordName;
            this.maxRows = maxRows;
            this.convertNames = convertNames;
            this.useLogicalTypes = useLogicalTypes;
            this.defaultPrecision = defaultPrecision;
            this.defaultScale = defaultScale;
            this.codec = codec;
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
            private CodecFactory codec = CodecFactory.nullCodec();

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

            public Builder codecFactory(String codec) {
                this.codec = AvroUtil.getCodecFactory(codec);
                return this;
            }

            public AvroConversionOptions build() {
                return new AvroConversionOptions(recordName, maxRows, convertNames, useLogicalTypes, defaultPrecision, defaultScale, codec);
            }
        }
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, final AvroConversionOptions options, final ResultSetRowCallback callback)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, options);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.setCodec(options.codec);
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
                            while (c >= 0) {
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
                            while (c >= 0) {
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

                    Object value;

                    // If a Timestamp type, try getTimestamp() rather than getObject()
                    if (javaSqlType == TIMESTAMP
                            || javaSqlType == TIMESTAMP_WITH_TIMEZONE
                            // The following are Oracle-specific codes for TIMESTAMP WITH TIME ZONE and TIMESTAMP WITH LOCAL TIME ZONE. This would be better
                            // located in the DatabaseAdapter interfaces, but some processors (like ExecuteSQL) use this method but don't specify a DatabaseAdapter.
                            || javaSqlType == -101
                            || javaSqlType == -102) {
                        try {
                            value = rs.getTimestamp(i);
                            // Some drivers (like Derby) return null for getTimestamp() but return a Timestamp object in getObject()
                            if (value == null) {
                                value = rs.getObject(i);
                            }
                        } catch (Exception e) {
                            // The cause of the exception is not known, but we'll fall back to call getObject() and handle any "real" exception there
                            value = rs.getObject(i);
                        }
                    } else {
                        value = rs.getObject(i);
                    }

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
                        } else if ((value instanceof Long) && meta.getPrecision(i) < MAX_DIGITS_IN_INT) {
                            int intValue = ((Long)value).intValue();
                            rec.put(i-1, intValue);
                        } else {
                            rec.put(i-1, value);
                        }

                    } else if (value instanceof Date) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof java.sql.SQLXML) {
                        rec.put(i - 1, ((SQLXML) value).getString());
                    } else {
                        // The different types that we support are numbers (int, long, double, float),
                        // as well as boolean values and Strings. Since Avro doesn't provide
                        // timestamp types, we want to convert those to Strings. So we will cast anything other
                        // than numbers or booleans to strings by using the toString() method.
                        rec.put(i - 1, value.toString());
                    }
                }
                try {
                    dataFileWriter.append(rec);
                    nrOfRows += 1;
                } catch (DataFileWriter.AppendWriteException awe) {
                    Throwable rootCause = ExceptionUtils.getRootCause(awe);
                    if(rootCause instanceof UnresolvedUnionException) {
                        UnresolvedUnionException uue = (UnresolvedUnionException) rootCause;
                        throw new RuntimeException(
                                "Unable to resolve union for value " + uue.getUnresolvedDatum() +
                                " with type " + uue.getUnresolvedDatum().getClass().getCanonicalName() +
                                " while appending record " + rec,
                                awe);
                    } else {
                        throw awe;
                    }
                }

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
                case OTHER:
                case SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) < MAX_DIGITS_IN_INT)) {
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
                case TIMESTAMP_WITH_TIMEZONE:
                case -101: // Oracle's TIMESTAMP WITH TIME ZONE
                case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE
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
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     *
     * @param stmt the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    public static void setParameters(final PreparedStatement stmt, final Map<String, String> attributes) throws SQLException {
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher matcher = SQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
            if (matcher.matches()) {
                final int parameterIndex = Integer.parseInt(matcher.group(1));

                final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                if (!isNumeric) {
                    throw new SQLDataException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral type");
                }

                final int jdbcType = Integer.parseInt(entry.getValue());
                final String valueAttrName = "sql.args." + parameterIndex + ".value";
                final String parameterValue = attributes.get(valueAttrName);
                final String formatAttrName = "sql.args." + parameterIndex + ".format";
                final String parameterFormat = attributes.containsKey(formatAttrName)? attributes.get(formatAttrName):"";

                try {
                    JdbcCommon.setParameter(stmt, valueAttrName, parameterIndex, parameterValue, jdbcType, parameterFormat);
                } catch (final NumberFormatException nfe) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted into the necessary data type", nfe);
                } catch (ParseException pe) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted to a timestamp", pe);
                } catch (UnsupportedEncodingException uee) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted to UTF-8", uee);
                }
            }
        }
    }

    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt the PreparedStatement to set the parameter on
     * @param attrName the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the SQL parameter to set
     * @param parameterValue the value of the SQL parameter to set
     * @param jdbcType the JDBC Type of the SQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    public static void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType,
                              final String valueFormat)
            throws SQLException, ParseException, UnsupportedEncodingException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            switch (jdbcType) {
                case Types.BIT:
                    stmt.setBoolean(parameterIndex, "1".equals(parameterValue) || "t".equalsIgnoreCase(parameterValue) || Boolean.parseBoolean(parameterValue));
                    break;
                case Types.BOOLEAN:
                    stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                    break;
                case Types.TINYINT:
                    stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                    break;
                case Types.SMALLINT:
                    stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                    break;
                case Types.INTEGER:
                    stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                    break;
                case Types.BIGINT:
                    stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                    break;
                case Types.REAL:
                    stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                    break;
                case Types.DATE:
                    java.sql.Date date;

                    if (valueFormat.equals("")) {
                        if(LONG_PATTERN.matcher(parameterValue).matches()){
                            date = new java.sql.Date(Long.parseLong(parameterValue));
                        }else {
                            String dateFormatString = "yyyy-MM-dd";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            date = new java.sql.Date(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalDate parsedDate = LocalDate.parse(parameterValue, dtFormatter);
                        date = new java.sql.Date(java.sql.Date.from(parsedDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());
                    }

                    stmt.setDate(parameterIndex, date);
                    break;
                case Types.TIME:
                    Time time;

                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            time = new Time(Long.parseLong(parameterValue));
                        } else {
                            String timeFormatString = "HH:mm:ss.SSS";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            time = new Time(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalTime parsedTime = LocalTime.parse(parameterValue, dtFormatter);
                        LocalDateTime localDateTime = parsedTime.atDate(LocalDate.ofEpochDay(0));
                        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                        time = new Time(instant.toEpochMilli());
                    }

                    stmt.setTime(parameterIndex, time);
                    break;
                case Types.TIMESTAMP:
                    long lTimestamp=0L;

                    // Backwards compatibility note: Format was unsupported for a timestamp field.
                    if (valueFormat.equals("")) {
                        if(LONG_PATTERN.matcher(parameterValue).matches()){
                            lTimestamp = Long.parseLong(parameterValue);
                        } else {
                            final SimpleDateFormat dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            lTimestamp = parsedDate.getTime();
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        TemporalAccessor accessor = dtFormatter.parse(parameterValue);
                        java.util.Date parsedDate = java.util.Date.from(Instant.from(accessor));
                        lTimestamp = parsedDate.getTime();
                    }

                    stmt.setTimestamp(parameterIndex, new Timestamp(lTimestamp));

                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bValue;

                    switch(valueFormat){
                        case "":
                        case "ascii":
                            bValue = parameterValue.getBytes("ASCII");
                            break;
                        case "hex":
                            bValue = DatatypeConverter.parseHexBinary(parameterValue);
                            break;
                        case "base64":
                            bValue = DatatypeConverter.parseBase64Binary(parameterValue);
                            break;
                        default:
                            throw new ParseException("Unable to parse binary data using the formatter `" + valueFormat + "`.",0);
                    }

                    stmt.setBinaryStream(parameterIndex, new ByteArrayInputStream(bValue), bValue.length);

                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                    stmt.setString(parameterIndex, parameterValue);
                    break;
                case Types.CLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setCharacterStream(parameterIndex, reader);
                    }
                    break;
                case Types.NCLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setNCharacterStream(parameterIndex, reader);
                    }
                    break;
                default:
                    stmt.setObject(parameterIndex, parameterValue, jdbcType);
                    break;
            }
        }
    }

    public static DateTimeFormatter getDateTimeFormatter(String pattern) {
        switch(pattern) {
            case "BASIC_ISO_DATE": return DateTimeFormatter.BASIC_ISO_DATE;
            case "ISO_LOCAL_DATE": return DateTimeFormatter.ISO_LOCAL_DATE;
            case "ISO_OFFSET_DATE": return DateTimeFormatter.ISO_OFFSET_DATE;
            case "ISO_DATE": return DateTimeFormatter.ISO_DATE;
            case "ISO_LOCAL_TIME": return DateTimeFormatter.ISO_LOCAL_TIME;
            case "ISO_OFFSET_TIME": return DateTimeFormatter.ISO_OFFSET_TIME;
            case "ISO_TIME": return DateTimeFormatter.ISO_TIME;
            case "ISO_LOCAL_DATE_TIME": return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            case "ISO_OFFSET_DATE_TIME": return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            case "ISO_ZONED_DATE_TIME": return DateTimeFormatter.ISO_ZONED_DATE_TIME;
            case "ISO_DATE_TIME": return DateTimeFormatter.ISO_DATE_TIME;
            case "ISO_ORDINAL_DATE": return DateTimeFormatter.ISO_ORDINAL_DATE;
            case "ISO_WEEK_DATE": return DateTimeFormatter.ISO_WEEK_DATE;
            case "ISO_INSTANT": return DateTimeFormatter.ISO_INSTANT;
            case "RFC_1123_DATE_TIME": return DateTimeFormatter.RFC_1123_DATE_TIME;
            default: return DateTimeFormatter.ofPattern(pattern);
        }
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToAvroStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
        void applyStateChanges();
    }

}
