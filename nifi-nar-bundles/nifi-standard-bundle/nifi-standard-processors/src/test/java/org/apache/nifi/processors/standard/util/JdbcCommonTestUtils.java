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

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcCommonTestUtils {
    static ResultSet resultSetReturningMetadata(ResultSetMetaData metadata) throws SQLException {
        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metadata);

        final AtomicInteger counter = new AtomicInteger(1);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return counter.getAndDecrement() > 0;
            }
        }).when(rs).next();

        return rs;
    }

    static InputStream convertResultSetToAvroInputStream(ResultSet rs) throws SQLException, IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        JdbcCommon.convertToAvroStream(rs, baos, false);

        final byte[] serializedBytes = baos.toByteArray();

        return new ByteArrayInputStream(serializedBytes);
    }
}
