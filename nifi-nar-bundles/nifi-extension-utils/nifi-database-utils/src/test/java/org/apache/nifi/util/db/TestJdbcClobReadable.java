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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

public class TestJdbcClobReadable {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    String createTable = "create table users ("
            + "  id int NOT NULL GENERATED ALWAYS AS IDENTITY, "
            + "  email varchar(255) NOT NULL UNIQUE, "
            + "  password varchar(255) DEFAULT NULL, "
            + "  someclob CLOB default null, "
            + "   PRIMARY KEY (id) ) ";

    String dropTable = "drop table users";

    @Test
    public void testClobWithChinese() throws SQLException, IOException, ClassNotFoundException {
        String chineseContent = "中国China";
        validateClob(chineseContent);
    }

    @Test
    public void testClobWithJapanese() throws SQLException, IOException, ClassNotFoundException {
        String japaneseContent = "にほんJapan";
        validateClob(japaneseContent);
    }

    @Test
    public void testClobWithKorean() throws SQLException, IOException, ClassNotFoundException {
        String koreanContent = "にほんKorea";
        validateClob(koreanContent);
    }

    // many test use Derby as database, so ensure driver is available
    @Test
    public void testDriverLoad() throws ClassNotFoundException {
        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        assertNotNull(clazz);
    }

    private void validateClob(String someClob) throws SQLException, ClassNotFoundException, IOException {
        folder.delete();

        final Connection con = createConnection(folder.getRoot().getAbsolutePath());
        final Statement st = con.createStatement();

        try {
            st.executeUpdate(dropTable);
        } catch (final Exception e) {
            // table may not exist, this is not serious problem.
        }

        st.executeUpdate(createTable);

        st.executeUpdate(String.format("insert into users (email, password, someClob) "
                + " values ('robert.gates@cold.com', '******', '%s')", someClob));

        final ResultSet resultSet = st.executeQuery("select U.SOMECLOB from users U");

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        JdbcCommon.convertToAvroStream(resultSet, outStream, false);

        final byte[] serializedBytes = outStream.toByteArray();
        assertNotNull(serializedBytes);
        System.out.println("Avro serialized result size in bytes: " + serializedBytes.length);

        st.close();
        con.close();

        // Deserialize bytes to records

        final InputStream inputStream = new ByteArrayInputStream(serializedBytes);

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                Assert.assertEquals("Unreadable code for this Clob value.", someClob, record.get("SOMECLOB").toString());
                System.out.println(record);
            }
        }
    }

    private Connection createConnection(String location) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        return DriverManager.getConnection("jdbc:derby:" + location + ";create=true");
    }
}
