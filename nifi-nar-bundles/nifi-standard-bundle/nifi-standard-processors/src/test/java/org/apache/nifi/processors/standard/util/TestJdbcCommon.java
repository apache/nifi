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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
        con.close();

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

        Assert.assertTrue(foundIntSchema);
        Assert.assertTrue(foundNullSchema);
    }

    @Test
    public void testUnsignedIntShouldBeLong() throws SQLException, IllegalArgumentException, IllegalAccessException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
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

        Assert.assertTrue(foundLongSchema);
        Assert.assertTrue(foundNullSchema);
    }


    @Test
    public void testConvertToAvroStreamForBigDecimal() throws SQLException, IOException {
        final ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metadata.getColumnName(1)).thenReturn("The.Chairman");
        when(metadata.getTableName(1)).thenReturn("1the::table");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return counter.getAndDecrement() > 0;
            }
        }).when(rs).next();

        final BigDecimal bigDecimal = new BigDecimal(38D);
        when(rs.getObject(Mockito.anyInt())).thenReturn(bigDecimal);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        JdbcCommon.convertToAvroStream(rs, baos, true);

        final byte[] serializedBytes = baos.toByteArray();

        final InputStream instream = new ByteArrayInputStream(serializedBytes);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                assertEquals("_1the__table", record.getSchema().getName());
                assertEquals(bigDecimal.toString(), record.get("The_Chairman").toString());
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
