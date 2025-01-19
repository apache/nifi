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
package org.apache.nifi.util.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nifi.util.db.JdbcCommon.MASKED_LOG_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJdbcCommon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestJdbcCommon.class);
    static final String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";

    /**
     * Setting up Connection is expensive operation.
     * So let's do this only once and reuse Connection in each test.
     */
    static protected Connection con;

    private File tempFile;

    @BeforeAll
    public static void beforeAll() throws ClassNotFoundException {
        final File derbyLog = new File(System.getProperty("java.io.tmpdir"), "derby.log");
        derbyLog.deleteOnExit();
        System.setProperty(DERBY_LOG_PROPERTY, derbyLog.getAbsolutePath());
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    }

    @AfterAll
    public static void clearDerbyLog() {
        System.clearProperty(DERBY_LOG_PROPERTY);
    }

    @BeforeEach
    public void setup() throws ClassNotFoundException, SQLException, IOException {
        DriverManager.registerDriver(new EmbeddedDriver());
        tempFile = new File(System.getProperty("java.io.tmpdir"), (this.getClass().getSimpleName() + "-" + UUID.randomUUID()));
        String location = tempFile.getAbsolutePath();
        con = DriverManager.getConnection("jdbc:derby:" + location + ";create=true");
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createTable);
        }
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (tempFile.exists()) {
            final SQLException exception = assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:;shutdown=true"));
            assertEquals("XJ015", exception.getSQLState());
            FileUtils.deleteFile(tempFile, true);
        }
    }

    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";

    @Test
    public void testCreateSchema() throws SQLException {
        final Statement st = con.createStatement();
        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        final ResultSet resultSet = st.executeQuery("select * from restaurants");

        final Schema schema = JdbcCommon.createSchema(resultSet);
        assertNotNull(schema);

        // records name, should be result set first column table name
        // Notice! sql select may join data from different tables, other columns
        // may have different table names
        assertEquals("RESTAURANTS", schema.getName());
        assertNotNull(schema.getField("ID"));
        assertNotNull(schema.getField("NAME"));
        assertNotNull(schema.getField("CITY"));

        st.close();
//        con.close();
    }

    @Test
    public void testCreateSchemaNoColumns() throws SQLException {

        final ResultSet resultSet = mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(0);
        when(resultSetMetaData.getTableName(1)).thenThrow(SQLException.class);

        final Schema schema = JdbcCommon.createSchema(resultSet);
        assertNotNull(schema);

        // records name, should be result set first column table name
        // Notice! sql select may join data from different tables, other columns
        // may have different table names
        assertEquals("NiFi_ExecuteSQL_Record", schema.getName());
        assertNull(schema.getField("ID"));
    }

    @Test
    public void testCreateSchemaNoTableName() throws SQLException {

        final ResultSet resultSet = mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getTableName(1)).thenReturn("");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(resultSetMetaData.getColumnName(1)).thenReturn("ID");

        final Schema schema = JdbcCommon.createSchema(resultSet);
        assertNotNull(schema);

        // records name, should be result set first column table name
        assertEquals("NiFi_ExecuteSQL_Record", schema.getName());

    }

    @Test
    public void testCreateSchemaOnlyColumnLabel() throws SQLException {

        final ResultSet resultSet = mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSetMetaData.getTableName(1)).thenReturn("TEST");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(resultSetMetaData.getColumnName(1)).thenReturn("");
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("ID");
        when(resultSetMetaData.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(resultSetMetaData.getColumnName(2)).thenReturn("VCHARC");
        when(resultSetMetaData.getColumnLabel(2)).thenReturn("NOT_VCHARC");

        final Schema schema = JdbcCommon.createSchema(resultSet);
        assertNotNull(schema);

        assertNotNull(schema.getField("ID"));
        assertNotNull(schema.getField("NOT_VCHARC"));

        // records name, should be result set first column table name
        assertEquals("TEST", schema.getName());
    }

    @Test
    public void testConvertToBytes() throws SQLException, IOException {
        final Statement st = con.createStatement();
        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        final ResultSet resultSet = st.executeQuery("select R.*, ROW_NUMBER() OVER () as rownr from restaurants R");

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        JdbcCommon.convertToAvroStream(resultSet, outStream, false);

        final byte[] serializedBytes = outStream.toByteArray();
        assertNotNull(serializedBytes);

        st.close();
    }


    @Test
    public void testCreateSchemaTypes() throws SQLException, IllegalArgumentException, IllegalAccessException {
        final Set<Integer> fieldsToIgnore = new HashSet<>();
        fieldsToIgnore.add(Types.NULL);
        fieldsToIgnore.add(Types.OTHER);

        final Field[] fieldTypes = Types.class.getFields();
        for (final Field field : fieldTypes) {
            final Object fieldObject = field.get(null);
            final int type = (int) fieldObject;

            if (fieldsToIgnore.contains(Types.NULL)) {
                continue;
            }

            final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
            when(metadata.getColumnCount()).thenReturn(1);
            when(metadata.getColumnType(1)).thenReturn(type);
            when(metadata.getColumnName(1)).thenReturn(field.getName());
            when(metadata.getTableName(1)).thenReturn("table");

            final ResultSet rs = mock(ResultSet.class);
            when(rs.getMetaData()).thenReturn(metadata);

            try {
                JdbcCommon.createSchema(rs);
            } catch (final IllegalArgumentException | SQLException sqle) {
                fail("Failed when using type " + field.getName());
            }
        }
    }

    @Test
    public void testSignedIntShouldBeInt() throws SQLException, IllegalArgumentException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.isSigned(1)).thenReturn(true);
        when(metadata.getColumnName(1)).thenReturn("Col1");
        when(metadata.getTableName(1)).thenReturn("Table1");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        Schema schema = JdbcCommon.createSchema(rs);
        assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        assertEquals(2, fieldSchema.getTypes().size());

        boolean foundIntSchema = false;
        boolean foundNullSchema = false;

        for (Schema type : fieldSchema.getTypes()) {
            if (type.getType().equals(Schema.Type.INT)) {
                foundIntSchema = true;
            } else if (type.getType().equals(Schema.Type.NULL)) {
                foundNullSchema = true;
            }
        }

        assertTrue(foundIntSchema);
        assertTrue(foundNullSchema);
    }

    @Test
    public void testUnsignedIntShouldBeLong() throws SQLException, IllegalArgumentException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.getPrecision(1)).thenReturn(10);
        when(metadata.isSigned(1)).thenReturn(false);
        when(metadata.getColumnName(1)).thenReturn("Col1");
        when(metadata.getTableName(1)).thenReturn("Table1");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        Schema schema = JdbcCommon.createSchema(rs);
        assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        assertEquals(2, fieldSchema.getTypes().size());

        boolean foundLongSchema = false;
        boolean foundNullSchema = false;

        for (Schema type : fieldSchema.getTypes()) {
            if (type.getType().equals(Schema.Type.LONG)) {
                foundLongSchema = true;
            } else if (type.getType().equals(Schema.Type.NULL)) {
                foundNullSchema = true;
            }
        }

        assertTrue(foundLongSchema);
        assertTrue(foundNullSchema);
    }

    @Test
    public void testMediumUnsignedIntShouldBeInt() throws SQLException, IllegalArgumentException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.getPrecision(1)).thenReturn(8);
        when(metadata.isSigned(1)).thenReturn(false);
        when(metadata.getColumnName(1)).thenReturn("Col1");
        when(metadata.getTableName(1)).thenReturn("Table1");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        Schema schema = JdbcCommon.createSchema(rs);
        assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        assertEquals(2, fieldSchema.getTypes().size());

        boolean foundIntSchema = false;
        boolean foundNullSchema = false;

        for (Schema type : fieldSchema.getTypes()) {
            if (type.getType().equals(Schema.Type.INT)) {
                foundIntSchema = true;
            } else if (type.getType().equals(Schema.Type.NULL)) {
                foundNullSchema = true;
            }
        }

        assertTrue(foundIntSchema);
        assertTrue(foundNullSchema);
    }

    @Test
    public void testInt9ShouldBeLong() throws SQLException, IllegalArgumentException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.getPrecision(1)).thenReturn(9);
        when(metadata.isSigned(1)).thenReturn(false);
        when(metadata.getColumnName(1)).thenReturn("Col1");
        when(metadata.getTableName(1)).thenReturn("Table1");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        Schema schema = JdbcCommon.createSchema(rs);
        assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        assertEquals(2, fieldSchema.getTypes().size());

        boolean foundLongSchema = false;
        boolean foundNullSchema = false;

        for (Schema type : fieldSchema.getTypes()) {
            if (type.getType().equals(Schema.Type.LONG)) {
                foundLongSchema = true;
            } else if (type.getType().equals(Schema.Type.NULL)) {
                foundNullSchema = true;
            }
        }

        assertTrue(foundLongSchema);
        assertTrue(foundNullSchema);
    }

    @Test
    public void testConvertToAvroStreamForBigDecimal() throws SQLException, IOException {
        final BigDecimal bigDecimal = new BigDecimal("12345");
        // If db returns a precision, it should be used.
        testConvertToAvroStreamForBigDecimal(bigDecimal, 38, 10, 38, 0);
    }

    @Test
    public void testConvertToAvroStreamForBigDecimalWithUndefinedPrecision() throws SQLException, IOException {
        final int expectedScale = 3;
        final int dbPrecision = 0;
        final BigDecimal bigDecimal = new BigDecimal(new BigInteger("12345"), expectedScale, new MathContext(dbPrecision));
        // If db doesn't return a precision, default precision should be used.
        testConvertToAvroStreamForBigDecimal(bigDecimal, dbPrecision, 24, 24, expectedScale);
    }

    @Test
    public void testConvertToAvroStreamForBigDecimalWithScaleLargerThanPrecision() throws SQLException, IOException {
        final int expectedScale = 6; // Scale can be larger than precision in Oracle
        final int dbPrecision = 5;
        final BigDecimal bigDecimal = new BigDecimal("0.000123", new MathContext(dbPrecision));
        // If db doesn't return a precision, default precision should be used.
        testConvertToAvroStreamForBigDecimal(bigDecimal, dbPrecision, 10, expectedScale, expectedScale);
    }

    @Test
    public void testConvertToAvroStreamForBigDecimalWithZeroScale() throws SQLException, IOException {
        final int dbPrecision = 5;
        final int dbScale = 0;

        final int expectedPrecision = dbPrecision;
        final int expectedScale = dbScale;

        final int defaultPrecision = 15;
        final int defaultScale = 15;

        final BigDecimal bigDecimal = new BigDecimal("1.123", new MathContext(dbPrecision));
        final BigDecimal expectedValue = BigDecimal.ONE;
        testConvertToAvroStreamForBigDecimal(bigDecimal, expectedValue, dbPrecision, dbScale, defaultPrecision, defaultScale, expectedPrecision, expectedScale);
    }

    private void testConvertToAvroStreamForBigDecimal(BigDecimal bigDecimal, int dbPrecision, int defaultPrecision, int expectedPrecision, int expectedScale) throws SQLException, IOException {
        testConvertToAvroStreamForBigDecimal(bigDecimal, bigDecimal, dbPrecision, expectedScale, defaultPrecision, -1, expectedPrecision, expectedScale);
    }

    private void testConvertToAvroStreamForBigDecimal(BigDecimal bigDecimal, BigDecimal expectedValue, int dbPrecision, int dbScale, int defaultPrecision, int defaultScale,
                                                      int expectedPrecision, int expectedScale) throws SQLException, IOException {

        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metadata.getColumnName(1)).thenReturn("The.Chairman");
        when(metadata.getTableName(1)).thenReturn("1the::table");
        when(metadata.getPrecision(1)).thenReturn(dbPrecision);
        when(metadata.getScale(1)).thenReturn(dbScale);

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        when(rs.getObject(Mockito.anyInt())).thenReturn(bigDecimal);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final JdbcCommon.AvroConversionOptions.Builder optionsBuilder = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(true).defaultPrecision(defaultPrecision);
        if (defaultScale > -1) optionsBuilder.defaultScale(defaultScale);

        final JdbcCommon.AvroConversionOptions options = optionsBuilder.build();
        JdbcCommon.convertToAvroStream(rs, baos, options, null);

        final byte[] serializedBytes = baos.toByteArray();

        final InputStream instream = new ByteArrayInputStream(serializedBytes);

        final GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(null, null, genericData);
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            final Schema generatedUnion = dataFileReader.getSchema().getField("The_Chairman").schema();
            // null and decimal.
            assertEquals(2, generatedUnion.getTypes().size());
            final LogicalType logicalType = generatedUnion.getTypes().get(1).getLogicalType();
            assertNotNull(logicalType);
            assertEquals("decimal", logicalType.getName());
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            assertEquals(expectedPrecision, decimalType.getPrecision());
            assertEquals(expectedScale, decimalType.getScale());

            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals("_1the__table", record.getSchema().getName());
                assertEquals(expectedValue, record.get("The_Chairman"));
            }
        }
    }

    @Test
    public void testClob() throws Exception {
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE TABLE clobtest (id INT, text CLOB(64 K))");
            stmt.execute("INSERT INTO clobtest VALUES (41, NULL)");
            PreparedStatement ps = con.prepareStatement("INSERT INTO clobtest VALUES (?, ?)");
            ps.setInt(1, 42);
            final char[] buffer = new char[4002];
            IntStream.range(0, 4002).forEach((i) -> buffer[i] = String.valueOf(i % 10).charAt(0));
            // Put a zero-byte in to test the buffer building logic
            buffer[1] = 0;
            final ReaderInputStream isr = ReaderInputStream.builder().setCharset(Charset.defaultCharset()).setReader(new CharArrayReader(buffer)).get();

            // - set the value of the input parameter to the input stream
            ps.setAsciiStream(2, isr, 4002);
            ps.execute();
            isr.close();

            final ResultSet resultSet = stmt.executeQuery("select * from clobtest");

            final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            JdbcCommon.convertToAvroStream(resultSet, outStream, false);

            final byte[] serializedBytes = outStream.toByteArray();
            assertNotNull(serializedBytes);

            // Deserialize bytes to records
            final InputStream instream = new ByteArrayInputStream(serializedBytes);

            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
                GenericRecord record = null;
                while (dataFileReader.hasNext()) {
                    // Reuse record object by passing it to next(). This saves us from
                    // allocating and garbage collecting many objects for files with
                    // many items.
                    record = dataFileReader.next(record);
                    Integer id = (Integer) record.get("ID");
                    Object o = record.get("TEXT");
                    if (id == 41) {
                        assertNull(o);
                    } else {
                        assertNotNull(o);
                        final String text = o.toString();
                        assertEquals(4002, text.length());
                        // Third character should be '2'
                        assertEquals('2', text.charAt(2));
                    }
                }
            }
        }
    }

    @Test
    public void testBlob() throws Exception {
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE TABLE blobtest (id INT, b BLOB(64 K))");
            stmt.execute("INSERT INTO blobtest VALUES (41, NULL)");
            PreparedStatement ps = con.prepareStatement("INSERT INTO blobtest VALUES (?, ?)");
            ps.setInt(1, 42);
            final byte[] buffer = new byte[4002];
            IntStream.range(0, 4002).forEach((i) -> buffer[i] = (byte) ((i % 10) + 65));
            // Put a zero-byte in to test the buffer building logic
            buffer[1] = 0;
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer);

            // - set the value of the input parameter to the input stream
            ps.setBlob(2, bais, 4002);
            ps.execute();
            bais.close();

            final ResultSet resultSet = stmt.executeQuery("select * from blobtest");

            final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            JdbcCommon.convertToAvroStream(resultSet, outStream, false);

            final byte[] serializedBytes = outStream.toByteArray();
            assertNotNull(serializedBytes);

            // Deserialize bytes to records
            final InputStream instream = new ByteArrayInputStream(serializedBytes);

            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
                GenericRecord record = null;
                while (dataFileReader.hasNext()) {
                    // Reuse record object by passing it to next(). This saves us from
                    // allocating and garbage collecting many objects for files with
                    // many items.
                    record = dataFileReader.next(record);
                    Integer id = (Integer) record.get("ID");
                    Object o = record.get("B");
                    if (id == 41) {
                        assertNull(o);
                    } else {
                        assertNotNull(o);
                        assertInstanceOf(ByteBuffer.class, o);
                        final byte[] blob = ((ByteBuffer) o).array();
                        assertEquals(4002, blob.length);
                        // Third byte should be 67 ('C')
                        assertEquals('C', blob[2]);
                    }
                }
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForClob_FreeNotSupported() throws SQLException, IOException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.CLOB);
        when(metadata.getColumnName(1)).thenReturn("t_clob");
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        final byte[] byteBuffer = "test clob".getBytes(StandardCharsets.UTF_8);
        final Reader reader = new InputStreamReader(new ByteArrayInputStream(byteBuffer));

        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        when(clob.length()).thenReturn((long) byteBuffer.length);
        doThrow(SQLFeatureNotSupportedException.class).when(clob).free();
        when(rs.getClob(Mockito.anyInt())).thenReturn(clob);

        final InputStream instream = JdbcCommonTestUtils.convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                Object o = record.get("t_clob");
                assertInstanceOf(Utf8.class, o);
                assertEquals("test clob", o.toString());
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForBlob_FreeNotSupported() throws SQLException, IOException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.BLOB);
        when(metadata.getColumnName(1)).thenReturn("t_blob");
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        final byte[] byteBuffer = "test blob".getBytes(StandardCharsets.UTF_8);
        when(rs.getObject(Mockito.anyInt())).thenReturn(byteBuffer);

        ByteArrayInputStream bais = new ByteArrayInputStream(byteBuffer);
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(bais);
        when(blob.length()).thenReturn((long) byteBuffer.length);
        doThrow(SQLFeatureNotSupportedException.class).when(blob).free();
        when(rs.getBlob(Mockito.anyInt())).thenReturn(blob);

        final InputStream instream = JdbcCommonTestUtils.convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                Object o = record.get("t_blob");
                assertInstanceOf(ByteBuffer.class, o);
                ByteBuffer bb = (ByteBuffer) o;
                assertEquals("test blob", new String(bb.array(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForShort() throws SQLException, IOException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.TINYINT);
        when(metadata.getColumnName(1)).thenReturn("t_int");
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        final short s = 25;
        when(rs.getObject(Mockito.anyInt())).thenReturn(s);

        final InputStream instream = JdbcCommonTestUtils.convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals(Short.toString(s), record.get("t_int").toString());
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForUnsignedIntegerWithPrecision1ReturnedAsLong_NIFI5612() throws SQLException, IOException {
        final String mockColumnName = "t_int";
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.isSigned(1)).thenReturn(false);
        when(metadata.getPrecision(1)).thenReturn(1);
        when(metadata.getColumnName(1)).thenReturn(mockColumnName);
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        final Long ret = 0L;
        when(rs.getObject(Mockito.anyInt())).thenReturn(ret);

        final InputStream instream = JdbcCommonTestUtils.convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals(Long.toString(ret), record.get(mockColumnName).toString());
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForUnsignedIntegerWithPrecision10() throws SQLException, IOException {
        final String mockColumnName = "t_int";
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.isSigned(1)).thenReturn(false);
        when(metadata.getPrecision(1)).thenReturn(10);
        when(metadata.getColumnName(1)).thenReturn(mockColumnName);
        when(metadata.getTableName(1)).thenReturn("table");

        final ResultSet rs = JdbcCommonTestUtils.resultSetReturningMetadata(metadata);

        final Long ret = 0L;
        when(rs.getObject(Mockito.anyInt())).thenReturn(ret);

        final InputStream instream = JdbcCommonTestUtils.convertResultSetToAvroInputStream(rs);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals(Long.toString(ret), record.get(mockColumnName).toString());
            }
        }
    }

    @Test
    public void testConvertToAvroStreamForDateTimeAsString() throws SQLException, IOException {
        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(false).build();

        testConvertToAvroStreamForDateTime(options,
                (record, date) -> assertEquals(new Utf8(date.toString()), record.get("date")),
                (record, time) -> assertEquals(new Utf8(time.toString()), record.get("time")),
                (record, timestamp) -> assertEquals(new Utf8(timestamp.toString()), record.get("timestamp"))
        );
    }

    @Test
    public void testConvertToAvroStreamForDateTimeAsLogicalType() throws SQLException, IOException {
        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(true).build();

        testConvertToAvroStreamForDateTime(options,
                (record, date) -> {
                    final int expectedDaysSinceEpoch = (int) date.toLocalDate().toEpochDay();
                    final int actualDaysSinceEpoch = (int) record.get("date");
                    LOGGER.debug("comparing days since epoch, expecting '{}', actual '{}'", expectedDaysSinceEpoch, actualDaysSinceEpoch);
                    assertEquals(expectedDaysSinceEpoch, actualDaysSinceEpoch);
                },
                (record, time) -> {
                    int millisSinceMidnight = (int) record.get("time");
                    LocalTime localTime = Instant.ofEpochMilli(millisSinceMidnight).atOffset(ZoneOffset.UTC).toLocalTime();
                    Time actual = Time.valueOf(localTime);
                    LOGGER.debug("comparing times, expecting '{}', actual '{}'", time, actual);
                    assertEquals(time, actual);
                },
                (record, timestamp) -> {
                    Timestamp actual = new Timestamp((long) record.get("timestamp"));
                    LOGGER.debug("comparing date/time, expecting '{}', actual '{}'", timestamp, actual);
                    assertEquals(timestamp, actual);
                }
        );
    }

    private void testConvertToAvroStreamForDateTime(
            JdbcCommon.AvroConversionOptions options, BiConsumer<GenericRecord, java.sql.Date> assertDate,
            BiConsumer<GenericRecord, Time> assertTime, BiConsumer<GenericRecord, Timestamp> assertTimeStamp)
            throws SQLException, IOException {

        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        when(metadata.getColumnCount()).thenReturn(3);
        when(metadata.getTableName(anyInt())).thenReturn("table");

        when(metadata.getColumnType(1)).thenReturn(Types.DATE);
        when(metadata.getColumnName(1)).thenReturn("date");
        final java.sql.Date date = java.sql.Date.valueOf("2017-05-10");
        when(rs.getObject(1)).thenReturn(date);

        when(metadata.getColumnType(2)).thenReturn(Types.TIME);
        when(metadata.getColumnName(2)).thenReturn("time");
        final Time time = Time.valueOf("12:34:56");
        when(rs.getObject(2)).thenReturn(time);

        when(metadata.getColumnType(3)).thenReturn(Types.TIMESTAMP);
        when(metadata.getColumnName(3)).thenReturn("timestamp");
        final Timestamp timestamp = Timestamp.valueOf("2017-05-11 19:59:39");
        when(rs.getObject(3)).thenReturn(timestamp);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer((Answer<Boolean>) invocation -> counter.getAndDecrement() > 0).when(rs).next();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        JdbcCommon.convertToAvroStream(rs, baos, options, null);

        final byte[] serializedBytes = baos.toByteArray();

        final InputStream instream = new ByteArrayInputStream(serializedBytes);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertDate.accept(record, date);
                assertTime.accept(record, time);
                assertTimeStamp.accept(record, timestamp);
            }
        }
    }

    // many test use Derby as database, so ensure driver is available
    @Test
    public void testDriverLoad() throws ClassNotFoundException {
        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        assertNotNull(clazz);
    }

    @Test
    public void testSetSensitiveParametersDoesNotLogSensitiveValues() throws SQLException {
        final Map<String, SensitiveValueWrapper> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", new SensitiveValueWrapper("4", false));
        attributes.put("sql.args.1.value", new SensitiveValueWrapper("123.4", true));
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE TABLE inttest (id INT)");
            PreparedStatement ps = con.prepareStatement("INSERT INTO inttest VALUES (?)");
            final SQLException exception = assertThrows(SQLException.class, () -> JdbcCommon.setSensitiveParameters(ps, attributes));
            assertTrue(exception.getMessage().contains(MASKED_LOG_VALUE));
            assertFalse(exception.getMessage().contains("123.4"));
        }
    }

    @Test
    public void testSetParametersDoesNotHaveSensitiveValues() throws SQLException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", "4");
        attributes.put("sql.args.1.value", "123.4");
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE TABLE inttest (id INT)");
            PreparedStatement ps = con.prepareStatement("INSERT INTO inttest VALUES (?)");
            final SQLException exception = assertThrows(SQLException.class, () -> JdbcCommon.setParameters(ps, attributes));
            assertFalse(exception.getMessage().contains(MASKED_LOG_VALUE));
            assertTrue(exception.getMessage().contains("123.4"));
        }
    }

}
