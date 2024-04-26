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
package org.apache.nifi.hive.metastore;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** This class is responsible for metastore init script processing and execution. */
public class ScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";

    private final Connection connection;

    public ScriptRunner(Connection connection) throws SQLException {
        this.connection = connection;
        if (!this.connection.getAutoCommit()) {
            // May throw SQLFeatureNotSupportedException which is a subclass of SQLException
            this.connection.setAutoCommit(true);
        }
    }

    public void runScript(Reader reader) throws IOException, SQLException {
        try {
            StringBuilder command = new StringBuilder();
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;
            while ((line = lineReader.readLine()) != null) {
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty() || trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
                    continue; //Skip comment line
                } else if (trimmedLine.endsWith(getDelimiter())) {
                    command.append(line, 0, line.lastIndexOf(getDelimiter()));
                    command.append(" ");
                    Statement statement = connection.createStatement();

                    statement.execute(command.toString());
                    connection.commit();

                    command = new StringBuilder();

                    statement.close();
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running metastore init script.", e);
        } finally {
            connection.rollback();
        }
    }

    private String getDelimiter() {
        return DEFAULT_DELIMITER;
    }
}
