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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJdbcTypesH2 {

    String createTable = "    CREATE TABLE `users` ( "
            + "  `id` int NOT NULL AUTO_INCREMENT, "
            + "  `email` varchar(255) NOT NULL, "
            + "  `password` varchar(255) DEFAULT NULL, "
            + "  `activation_code` varchar(255) DEFAULT NULL, "
            + "  `forgotten_password_code` varchar(255) DEFAULT NULL, "
            + "  `forgotten_password_time` datetime DEFAULT NULL, "
            + "  `created` datetime NOT NULL, "
            + "  `active` tinyint NOT NULL DEFAULT '0', "
            + "  `home_module_id` int DEFAULT NULL, "

            + "  somebinary BINARY(4) default null, "
            + "  somebinary2 VARBINARY default null, "
            + "  somebinary3 LONGVARBINARY default null, "
            + "  somearray INTEGER ARRAY default null, "
            + "  someblob BLOB default null, "
            + "  someclob CLOB default null, "

            + "  PRIMARY KEY (`id`), "
            + "  CONSTRAINT unique_email UNIQUE (`email`) ) ";
//            + "  KEY `home_module_id` (`home_module_id`) )" ;
/*            + "  CONSTRAINT `users_ibfk_1` FOREIGN KEY (`home_module_id`) REFERENCES "
            + "`modules` (`id`) ON DELETE SET NULL "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 " ;
  */

    String dropTable = "drop table users";

    String dbPath;

    @TempDir
    private Path tempDir;

    @BeforeEach
    public void beforeEach() throws IOException {
        dbPath = tempDir.resolve("db")
                .toFile()
                .getAbsolutePath();
    }

    @Test
    public void testSQLTypesMapping() throws SQLException, IOException {
        final Connection con = createConnection(dbPath);
        final Statement st = con.createStatement();

        try {
            st.executeUpdate(dropTable);
        } catch (final Exception ignored) {
            // table may not exist, this is not serious problem.
        }

        st.executeUpdate(createTable);

        st.executeUpdate("insert into users (email, password, activation_code, created, active, somebinary, somebinary2, somebinary3, someblob, someclob) "
                + " values ('mari.gates@cold.com', '******', 'CAS', '2005-12-03', 3, 0x66FF, 'ABDF', 'EE64', 'BB22', 'CC88')");

        final ResultSet resultSet = st.executeQuery("select U.*, ROW_NUMBER() OVER () as rownr from users U");

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        JdbcCommon.convertToAvroStream(resultSet, outStream, false);

        final byte[] serializedBytes = outStream.toByteArray();
        Assertions.assertNotNull(serializedBytes);

        st.close();
        con.close();
    }

    // verify H2 driver loading and get Connections works
    @Test
    public void testDriverLoad() throws SQLException {
//        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

        Connection con = createConnection(dbPath);

        Assertions.assertNotNull(con);
        con.close();
    }

    private Connection createConnection(String location) throws SQLException {

//        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        String connectionString = "jdbc:h2:file:" + location + "/testdb7";
        return DriverManager.getConnection(connectionString, "SA", "");
    }

}
