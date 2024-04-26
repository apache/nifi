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
package org.apache.nifi.processors.standard.calcite;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.standard.QueryRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRecordResultSetOutputStreamCallback {

    @Test
    void testResultSetClosed() throws IOException, SQLException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);

        final String writerId = "record-writer";
        runner.setProperty(writerId, writerId);

        final RecordSetWriterFactory writerService = new CSVRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.setProperty(writerService, "schema-access-strategy", "inherit-record-schema");
        runner.enableControllerService(writerService);

        final ResultSet resultSet = getResultSet();

        final RecordField fieldFirst = new RecordField("first", RecordFieldType.STRING.getDataType());
        final RecordField fieldLast = new RecordField("last", RecordFieldType.STRING.getDataType());
        final RecordSchema writerSchema = new SimpleRecordSchema(Arrays.asList(fieldFirst, fieldLast));

        final FlowFile flowFile = mock(FlowFile.class);
        when(flowFile.getAttributes()).thenReturn(new LinkedHashMap<>());

        final RecordResultSetOutputStreamCallback writer = new RecordResultSetOutputStreamCallback(runner.getLogger(),
                resultSet, writerSchema, 0, 0, writerService, flowFile.getAttributes());

        final OutputStream os = new ByteArrayOutputStream();
        writer.process(os);

        assertTrue(resultSet.isClosed());
    }

    private ResultSet getResultSet() throws SQLException {
        DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        final Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        calciteConnection.getRootSchema().add("TEST", new ReflectiveSchema(new CalciteTestSchema()));
        final Statement statement = calciteConnection.createStatement();
        return statement.executeQuery("SELECT * FROM TEST.PERSONS");
    }

    public static class Person {
        public final String first;
        public final String last;

        public Person(final String first, final String last) {
            this.first = first;
            this.last = last;
        }
    }

    public static class CalciteTestSchema extends AbstractSchema {
        public Person[] PERSONS = { new Person("Joe", "Smith"), new Person("Bob", "Jones") };
    }
}
