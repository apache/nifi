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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestJdbcCommon {

    static final String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";
    static final String dropTable = "drop table restaurants";

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    /**
     * Setting up Connection is expensive operation.
     * So let's do this only once and reuse Connection in each test.
     */
    static protected Connection con;

    @BeforeClass
    public static void setup() throws ClassNotFoundException, SQLException {
        System.setProperty("derby.stream.error.file", "target/derby.log");
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        // remove previous test database, if any
        folder.delete();

        String location = folder.getRoot().getAbsolutePath();
        con = DriverManager.getConnection("jdbc:derby:" + location + ";create=true");
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createTable);
        }
    }

    @Test
    public void testCreateSchema() throws ClassNotFoundException, SQLException {
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
    public void testCreateSchemaNoColumns() throws ClassNotFoundException, SQLException {

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
    public void testCreateSchemaNoTableName() throws ClassNotFoundException, SQLException {

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
    public void testCreateSchemaOnlyColumnLabel() throws ClassNotFoundException, SQLException {

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
    public void testConvertToBytes() throws ClassNotFoundException, SQLException, IOException {
        final Statement st = con.createStatement();
        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        final ResultSet resultSet = st.executeQuery("select R.*, ROW_NUMBER() OVER () as rownr from restaurants R");

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        JdbcCommon.convertToAvroStream(resultSet, outStream, false);

        final byte[] serializedBytes = outStream.toByteArray();
        assertNotNull(serializedBytes);
        System.out.println("Avro serialized result size in bytes: " + serializedBytes.length);

        st.close();

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
                System.out.println(record);
            }
        }
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
                sqle.printStackTrace();
                Assert.fail("Failed when using type " + field.getName());
            }
        }
    }

    @Test
    public void testSignedIntShouldBeInt() throws SQLException, IllegalArgumentException, IllegalAccessException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.isSigned(1)).thenReturn(true);
        when(metadata.getColumnName(1)).thenReturn("Col1");
        when(metadata.getTableName(1)).thenReturn("Table1");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        Schema schema = JdbcCommon.createSchema(rs);
        Assert.assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        Assert.assertEquals(2, fieldSchema.getTypes().size());

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
    public void testUnsignedIntShouldBeLong() throws SQLException, IllegalArgumentException, IllegalAccessException {
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
        Assert.assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        Assert.assertEquals(2, fieldSchema.getTypes().size());

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
    public void testMediumUnsignedIntShouldBeInt() throws SQLException, IllegalArgumentException, IllegalAccessException {
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
        Assert.assertNotNull(schema);

        Schema.Field field = schema.getField("Col1");
        Schema fieldSchema = field.schema();
        Assert.assertEquals(2, fieldSchema.getTypes().size());

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
    public void testConvertToAvroStreamForBigDecimal() throws SQLException, IOException {
        final BigDecimal bigDecimal = new BigDecimal(12345D);
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

    private void testConvertToAvroStreamForBigDecimal(BigDecimal bigDecimal, int dbPrecision, int defaultPrecision, int expectedPrecision, int expectedScale) throws SQLException, IOException {

        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metadata.getColumnName(1)).thenReturn("The.Chairman");
        when(metadata.getTableName(1)).thenReturn("1the::table");
        when(metadata.getPrecision(1)).thenReturn(dbPrecision);
        when(metadata.getScale(1)).thenReturn(expectedScale);

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return counter.getAndDecrement() > 0;
            }
        }).when(rs).next();

        when(rs.getObject(Mockito.anyInt())).thenReturn(bigDecimal);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(true).defaultPrecision(defaultPrecision).build();
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
                assertEquals(bigDecimal, record.get("The_Chairman"));
            }
        }
    }

    @Test
    public void testClob() throws Exception {
        try (final Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE TABLE clobtest (id INT, text CLOB(64 K))");
            stmt.execute("INSERT INTO blobtest VALUES (41, NULL)");
            PreparedStatement ps = con.prepareStatement("INSERT INTO clobtest VALUES (?, ?)");
            ps.setInt(1, 42);
            final char[] buffer = new char[4002];
            IntStream.range(0, 4002).forEach((i) -> buffer[i] = String.valueOf(i % 10).charAt(0));
            ReaderInputStream isr = new ReaderInputStream(new CharArrayReader(buffer), Charset.defaultCharset());

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
                        assertEquals(4002, o.toString().length());
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
                        assertTrue(o instanceof ByteBuffer);
                        assertEquals(4002, ((ByteBuffer) o).array().length);
                    }
                }
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

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return counter.getAndDecrement() > 0;
            }
        }).when(rs).next();

        final short s = 25;
        when(rs.getObject(Mockito.anyInt())).thenReturn(s);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        JdbcCommon.convertToAvroStream(rs, baos, false);

        final byte[] serializedBytes = baos.toByteArray();

        final InputStream instream = new ByteArrayInputStream(serializedBytes);

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
    public void testConvertToAvroStreamForDateTimeAsString() throws SQLException, IOException, ParseException {
        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(false).build();

        testConvertToAvroStreamForDateTime(options,
                (record, date) -> assertEquals(new Utf8(date.toString()), record.get("date")),
                (record, time) -> assertEquals(new Utf8(time.toString()), record.get("time")),
                (record, timestamp) -> assertEquals(new Utf8(timestamp.toString()), record.get("timestamp"))
        );
    }

    @Test
    public void testConvertToAvroStreamForDateTimeAsLogicalType() throws SQLException, IOException, ParseException {
        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions
                .builder().convertNames(true).useLogicalTypes(true).build();

        testConvertToAvroStreamForDateTime(options,
                (record, date) -> {
                    final int daysSinceEpoch = (int) record.get("date");
                    final long millisSinceEpoch = TimeUnit.MILLISECONDS.convert(daysSinceEpoch, TimeUnit.DAYS);
                    assertEquals(date, new java.sql.Date(millisSinceEpoch));
                },
                (record, time) -> assertEquals(time, new Time((int) record.get("time"))),
                (record, timestamp) -> assertEquals(timestamp, new Timestamp((long) record.get("timestamp")))
        );
    }

    private void testConvertToAvroStreamForDateTime(
            JdbcCommon.AvroConversionOptions options, BiConsumer<GenericRecord, java.sql.Date> assertDate,
            BiConsumer<GenericRecord, Time> assertTime, BiConsumer<GenericRecord, Timestamp> assertTimeStamp)
            throws SQLException, IOException, ParseException {

        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        BiFunction<String, String, Long> toMillis = (format, dateStr) -> {
            try {
                final SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                return dateFormat.parse(dateStr).getTime();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        };

        when(metadata.getColumnCount()).thenReturn(3);
        when(metadata.getTableName(anyInt())).thenReturn("table");

        when(metadata.getColumnType(1)).thenReturn(Types.DATE);
        when(metadata.getColumnName(1)).thenReturn("date");
        final java.sql.Date date = new java.sql.Date(toMillis.apply("yyyy/MM/dd", "2017/05/10"));
        when(rs.getObject(1)).thenReturn(date);

        when(metadata.getColumnType(2)).thenReturn(Types.TIME);
        when(metadata.getColumnName(2)).thenReturn("time");
        final Time time = new Time(toMillis.apply("HH:mm:ss.SSS", "12:34:56.789"));
        when(rs.getObject(2)).thenReturn(time);

        when(metadata.getColumnType(3)).thenReturn(Types.TIMESTAMP);
        when(metadata.getColumnName(3)).thenReturn("timestamp");
        final Timestamp timestamp = new Timestamp(toMillis.apply("yyyy/MM/dd HH:mm:ss.SSS", "2017/05/11 19:59:39.123"));
        when(rs.getObject(3)).thenReturn(timestamp);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return counter.getAndDecrement() > 0;
            }
        }).when(rs).next();

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

}
