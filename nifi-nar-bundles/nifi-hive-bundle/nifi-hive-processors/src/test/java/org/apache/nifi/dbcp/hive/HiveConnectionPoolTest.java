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

package org.apache.nifi.dbcp.hive;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveConnectionPoolTest {
    private UserGroupInformation userGroupInformation;
    private HiveConnectionPool hiveConnectionPool;
    private BasicDataSource basicDataSource;
    private ComponentLog componentLog;

    @Before
    public void setup() throws Exception {
        userGroupInformation = mock(UserGroupInformation.class);
        basicDataSource = mock(BasicDataSource.class);
        componentLog = mock(ComponentLog.class);

        when(userGroupInformation.doAs(isA(PrivilegedExceptionAction.class))).thenAnswer(invocation -> {
            try {
                return ((PrivilegedExceptionAction) invocation.getArguments()[0]).run();
            } catch (IOException |Error|RuntimeException|InterruptedException e) {
                throw e;
            } catch (Throwable e) {
                throw new UndeclaredThrowableException(e);
            }
        });
        initPool();
    }

    private void initPool() throws Exception {
        hiveConnectionPool = new HiveConnectionPool();

        Field ugiField = HiveConnectionPool.class.getDeclaredField("ugi");
        ugiField.setAccessible(true);
        ugiField.set(hiveConnectionPool, userGroupInformation);

        Field dataSourceField = HiveConnectionPool.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(hiveConnectionPool, basicDataSource);

        Field componentLogField = AbstractControllerService.class.getDeclaredField("logger");
        componentLogField.setAccessible(true);
        componentLogField.set(hiveConnectionPool, componentLog);
    }

    @Test(expected = ProcessException.class)
    public void testGetConnectionSqlException() throws SQLException {
        SQLException sqlException = new SQLException("bad sql");
        when(basicDataSource.getConnection()).thenThrow(sqlException);
        try {
            hiveConnectionPool.getConnection();
        } catch (ProcessException e) {
            assertEquals(sqlException, e.getCause());
            throw e;
        }
    }
}
