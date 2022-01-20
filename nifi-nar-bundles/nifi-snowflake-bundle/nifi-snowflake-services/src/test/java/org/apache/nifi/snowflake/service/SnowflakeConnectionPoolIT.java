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
package org.apache.nifi.snowflake.service;

import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Set the following constants:<p>
 * SNOWFLAKE_URL<p>
 * SNOWFLAKE_USER<p>
 * SNOWFLAKE_PASSWORD<p>
 * TABLE_NAME<p>
 */
public class SnowflakeConnectionPoolIT {
    public static final String SNOWFLAKE_URL = "account.snowflakecomputing.com";
    public static final String SNOWFLAKE_USER = "???";
    public static final String SNOWFLAKE_PASSWORD = "???";
    public static final String TABLE_NAME = "test_db.public.test_table";

    private static final String SERVICE_ID = SnowflakeComputingConnectionPool.class.getName();

    private TestRunner runner;

    private SnowflakeComputingConnectionPool service;

    @BeforeEach
    void setUp() throws Exception {
        service = new SnowflakeComputingConnectionPool();
        runner = TestRunners.newTestRunner(new AbstractSessionFactoryProcessor() {
            @Override
            public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
            }
        });
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, SnowflakeComputingConnectionPool.SNOWFLAKE_URL, SNOWFLAKE_URL);
        runner.setProperty(service, SnowflakeComputingConnectionPool.SNOWFLAKE_USER, SNOWFLAKE_USER);
        runner.setProperty(service, SnowflakeComputingConnectionPool.SNOWFLAKE_PASSWORD, SNOWFLAKE_PASSWORD);
        runner.setProperty(service, "loginTimeout", "5");
    }

    @Test
    void testReadSnowflakeTable() throws Exception {
        runner.assertValid(service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (
            final Connection connection = service.getConnection();
            final Statement st = connection.createStatement()
        ) {
            ResultSet resultSet = st.executeQuery("select * from " + TABLE_NAME);

            int nrOfRows = 0;
            while (resultSet.next()) {
                nrOfRows++;
            }

            System.out.println("Read " + nrOfRows + " records from " + TABLE_NAME);
        }
    }
}
