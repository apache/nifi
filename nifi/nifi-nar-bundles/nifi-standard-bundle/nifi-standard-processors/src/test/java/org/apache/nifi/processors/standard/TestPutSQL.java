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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPutSQL {
    static String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100), code integer)";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		System.setProperty("derby.stream.error.file", "target/derby.log");
	}

	@Test
	public void testDirectStatements() throws InitializationException, ProcessException, SQLException, IOException {
		final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
		final File tempDir = folder.getRoot();
		final File dbDir = new File(tempDir, "db");
		final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
		runner.addControllerService("dbcp", service);
		runner.enableControllerService(service);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				stmt.executeUpdate(createPersons);
			}
		}

		runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
		runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (1, 'Mark', 84)".getBytes());
		runner.run();

		runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
				assertEquals("Mark", rs.getString(2));
				assertEquals(84, rs.getInt(3));
				assertFalse(rs.next());
			}
		}

		runner.enqueue("UPDATE PERSONS SET NAME='George' WHERE ID=1".getBytes());
		runner.run();

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
				assertEquals("George", rs.getString(2));
				assertEquals(84, rs.getInt(3));
				assertFalse(rs.next());
			}
		}
	}


	@Test
	public void testStatementsWithPreparedParameters() throws InitializationException, ProcessException, SQLException, IOException {
		final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
		final File tempDir = folder.getRoot();
		final File dbDir = new File(tempDir, "db");
		final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
		runner.addControllerService("dbcp", service);
		runner.enableControllerService(service);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				stmt.executeUpdate(createPersons);
			}
		}

		runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
		final Map<String, String> attributes = new HashMap<>();
		attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.1.value", "1");

		attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
		attributes.put("sql.args.2.value", "Mark");

		attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.3.value", "84");

		runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)".getBytes(), attributes);
		runner.run();

		runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
				assertEquals("Mark", rs.getString(2));
				assertEquals(84, rs.getInt(3));
				assertFalse(rs.next());
			}
		}

		runner.clearTransferState();

		attributes.clear();
		attributes.put("sql.args.1.type", String.valueOf(Types.VARCHAR));
		attributes.put("sql.args.1.value", "George");

		attributes.put("sql.args.2.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.2.value", "1");

		runner.enqueue("UPDATE PERSONS SET NAME=? WHERE ID=?".getBytes(), attributes);
		runner.run();
		runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
				assertEquals("George", rs.getString(2));
				assertEquals(84, rs.getInt(3));
				assertFalse(rs.next());
			}
		}
	}


	@Test
	public void testMultipleStatementsWithinFlowFile() throws InitializationException, ProcessException, SQLException, IOException {
		final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
		final File tempDir = folder.getRoot();
		final File dbDir = new File(tempDir, "db");
		final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
		runner.addControllerService("dbcp", service);
		runner.enableControllerService(service);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				stmt.executeUpdate(createPersons);
			}
		}

		runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

		final String sql = "INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?); " +
				"UPDATE PERSONS SET NAME='George' WHERE ID=?; ";
		final Map<String, String> attributes = new HashMap<>();
		attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.1.value", "1");

		attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
		attributes.put("sql.args.2.value", "Mark");

		attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.3.value", "84");

		attributes.put("sql.args.4.type", String.valueOf(Types.INTEGER));
		attributes.put("sql.args.4.value", "1");

		runner.enqueue(sql.getBytes(), attributes);
		runner.run();

		// should fail because of the semicolon
		runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 1);

		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertFalse(rs.next());
			}
		}
	}


	/**
	 * Simple implementation only for testing purposes
	 */
	private static class MockDBCPService extends AbstractControllerService implements DBCPService {
		private final String dbLocation;

		public MockDBCPService(final String dbLocation) {
			this.dbLocation = dbLocation;
		}

		@Override
		public String getIdentifier() {
			return "dbcp";
		}

		@Override
		public Connection getConnection() throws ProcessException {
			try {
				Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
				final Connection con = DriverManager.getConnection("jdbc:derby:" + dbLocation + ";create=true");
				return con;
			} catch (final Exception e) {
				e.printStackTrace();
				throw new ProcessException("getConnection failed: " + e);
			}
		}
	}
}
