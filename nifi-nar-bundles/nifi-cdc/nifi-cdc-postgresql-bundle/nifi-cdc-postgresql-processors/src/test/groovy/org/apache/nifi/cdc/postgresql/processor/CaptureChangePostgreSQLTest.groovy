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
package org.apache.nifi.cdc.postgresql.processors;

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.internal.configuration.injection.filter.MockCandidateFilter

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.TimeoutException
import java.util.regex.Matcher
import java.util.regex.Pattern

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.ArgumentMatchers.anyString
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

/**
 * Unit test(s) for PostgreSQL CDC
 */
public class CaptureChangePostgreSQLTest {
    CaptureChangePostgreSQL processor
    TestRunner testRunner
    //MockBinlogClient client

    @Before
    void setUp() throws Exception {
        processor = new MockCaptureChangePostgreSQL()
        testRunner = TestRunners.newTestRunner(processor)
       // client = new MockBinlogClient('localhost', 3306, 'root', 'password')
    }

    @After
    void tearDown() throws Exception {

    }
    
    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CaptureChangePostgreSQL.class);
    }

    @Test
    public void testProcessor() {

    }


    
    class MockCaptureChangePostgreSQL extends CaptureChangePostgreSQL {
        
                       
                @Override
                protected void registerDriver(String locationString, String drvName) throws InitializationException {
                }
        
            }

}
