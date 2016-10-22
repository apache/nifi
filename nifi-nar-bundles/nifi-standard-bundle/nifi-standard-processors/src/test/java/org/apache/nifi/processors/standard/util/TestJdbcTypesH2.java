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

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestJdbcTypesH2 {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    String createTable = "    CREATE TABLE `users` ( "
            + "  `id` int(11) NOT NULL AUTO_INCREMENT, "
            + "  `email` varchar(255) NOT NULL, "
            + "  `password` varchar(255) DEFAULT NULL, "
            + "  `activation_code` varchar(255) DEFAULT NULL, "
            + "  `forgotten_password_code` varchar(255) DEFAULT NULL, "
            + "  `forgotten_password_time` datetime DEFAULT NULL, "
            + "  `created` datetime NOT NULL, "
            + "  `active` tinyint(1) NOT NULL DEFAULT '0', "
            + "  `home_module_id` int(11) DEFAULT NULL, "

            + "  somebinary BINARY default null, "
            + "  somebinary2 VARBINARY default null, "
            + "  somebinary3 LONGVARBINARY default null, "
            + "  somearray   ARRAY default null, "
            + "  someblob BLOB default null, "
            + "  someclob CLOB default null, "

            + "  PRIMARY KEY (`id`), "
            + "  UNIQUE KEY `email` (`email`) ) " ;
//            + "  KEY `home_module_id` (`home_module_id`) )" ;
/*            + "  CONSTRAINT `users_ibfk_1` FOREIGN KEY (`home_module_id`) REFERENCES "
            + "`modules` (`id`) ON DELETE SET NULL "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 " ;
  */

    String dropTable = "drop table users";

    @Test
    public void testSQLTypesMapping() throws ClassNotFoundException, SQLException, IOException {
        final Connection con = createConnection(folder.getRoot().getAbsolutePath());
        final Statement st = con.createStatement();

        try {
            st.executeUpdate(dropTable);
        } catch (final Exception e) {
            // table may not exist, this is not serious problem.
        }

        st.executeUpdate(createTable);

//        st.executeUpdate("insert into users (email, password, activation_code, forgotten_password_code, forgotten_password_time, created, active, home_module_id) "
//                + " values ('robert.gates@cold.com', '******', 'CAS', 'ounou', '2005-12-09', '2005-12-03', 1, 5)");

        st.executeUpdate("insert into users (email, password, activation_code, created, active, somebinary, somebinary2, somebinary3, someblob, someclob) "
                + " values ('mari.gates@cold.com', '******', 'CAS', '2005-12-03', 3, '66FF', 'ABDF', 'EE64', 'BB22', 'CC88')");

        final ResultSet resultSet = st.executeQuery("select U.*, ROW_NUMBER() OVER () as rownr from users U");
//      final ResultSet resultSet = st.executeQuery("select U.active from users U");
//        final ResultSet resultSet = st.executeQuery("select U.somebinary from users U");

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

    // verify H2 driver loading and get Connections works
    @Test
    public void testDriverLoad() throws ClassNotFoundException, SQLException {
//        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

        Connection con = createConnection(folder.getRoot().getAbsolutePath());

        assertNotNull(con);
        con.close();
    }

    private Connection createConnection(String location) throws ClassNotFoundException, SQLException {

//        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        String connectionString = "jdbc:h2:file:" + location + "/testdb7";
        return DriverManager.getConnection(connectionString, "SA", "");
    }

}
