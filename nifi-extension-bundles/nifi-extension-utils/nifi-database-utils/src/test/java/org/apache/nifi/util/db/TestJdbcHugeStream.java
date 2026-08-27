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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test streaming using large number of result set rows. 1. Read data from
 * database. 2. Create Avro schema from ResultSet meta data. 3. Read rows from
 * ResultSet and write rows to Avro writer stream (Avro will create record for
 * each row). 4. And finally read records from Avro stream to verify all data is
 * present in Avro stream.
 * Sql query will return all combinations from 3 table. For example when each
 * table contain 1000 rows, result set will be 1 000 000 000 rows.
 */
class TestJdbcHugeStream extends AbstractConnectionTest {

    @Test
    public void readSend2StreamHuge_FileBased() throws SQLException, IOException {
        try (final Connection con = getConnection()) {
            loadTestData2Database(con, 100, 100, 100);

            try (final Statement st = con.createStatement()) {
                // Notice!
                // Following select is deliberately invalid!
                // For testing we need huge amount of rows, so where part is not
                // used.
                final ResultSet resultSet = st.executeQuery("select "
                    + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
                    + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
                    + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
                    + ", ROW_NUMBER() OVER () as rownr "
                    + " from persons PER, products PRD, relationships REL");

                final OutputStream outStream = new FileOutputStream("target/data.avro");
                final long nrOfRows = JdbcCommon.convertToAvroStream(resultSet, outStream, false);

                // Deserialize bytes to records
                final InputStream instream = new FileInputStream("target/data.avro");

                final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(instream, datumReader)) {
                    GenericRecord record = null;
                    long recordsFromStream = 0;
                    while (dataFileReader.hasNext()) {
                        // Reuse record object by passing it to next(). This
                        // saves us from
                        // allocating and garbage collecting many objects for
                        // files with many items.
                        record = dataFileReader.next(record);
                        recordsFromStream += 1;
                    }
                    assertEquals(nrOfRows, recordsFromStream);
                }
            }
        }
    }

    // ================================================ helpers
    // ===============================================

    static final String DROP_PERSONS = "drop table persons";
    static final String DROP_PRODUCTS = "drop table products";
    static final String DROP_RELATIONSHIPS = "drop table relationships";
    static final String CREATE_PERSONS = "create table persons (id integer, name varchar(100), code integer)";
    static final String CREATE_PRODUCTS = "create table products (id integer, name varchar(100), code integer)";
    static final String CREATE_RELATIONSHIPS = "create table relationships (id integer,name varchar(100), code integer)";

    public static void loadTestData2Database(Connection con, int nrOfPersons, int nrOfProducts, int nrOfRels) throws SQLException {

        final Statement st = con.createStatement();

        // tables may not exist, this is not serious problem.
        try {
            st.executeUpdate(DROP_PERSONS);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(DROP_PRODUCTS);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(DROP_RELATIONSHIPS);
        } catch (final Exception ignored) {
        }

        st.executeUpdate(CREATE_PERSONS);
        st.executeUpdate(CREATE_PRODUCTS);
        st.executeUpdate(CREATE_RELATIONSHIPS);

        for (int i = 0; i < nrOfPersons; i++) {
            loadPersons(st, i);
        }

        for (int i = 0; i < nrOfProducts; i++) {
            loadProducts(st, i);
        }

        for (int i = 0; i < nrOfRels; i++) {
            loadRelationships(st, i);
        }

        st.close();
    }

    static final Random RNG = new Random(53495);

    private static void loadPersons(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into persons values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static void loadProducts(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into products values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static void loadRelationships(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into relationships values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static String createRandomName() {
        return createRandomString() + " " + createRandomString();
    }

    private static String createRandomString() {

        final int length = RNG.nextInt(10);
        final String characters = "ABCDEFGHIJ";

        final char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(RNG.nextInt(characters.length()));
        }
        return new String(text);
    }
}
