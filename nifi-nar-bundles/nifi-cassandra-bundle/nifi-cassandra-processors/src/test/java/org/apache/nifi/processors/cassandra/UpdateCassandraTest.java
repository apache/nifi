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

package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateCassandraTest {
    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(new MockDeleteCassandra());
        runner.setProperty(UpdateCassandra.CONTACT_POINTS, "localhost:9042");
        runner.setProperty(UpdateCassandra.KEYSPACE, "test");
        runner.assertValid();
    }

    @Test
    public void testDeleteFromProperty() {
        runner.setProperty(UpdateCassandra.QUERY, "${delete_query}");
        runner.enqueue("", new HashMap<String, String>(){{
            put("delete_query", "DELETE FROM test_table");
        }});
        runner.run();

        assertCount(1, 0);

        runner.setIncomingConnection(false);

        runner.setProperty(UpdateCassandra.QUERY, "DELETE FROM test_table");
        runner.run();

        assertCount(0, 0);

        runner.setVariable("vreg.delete_query", "DELETE FROM test_table");
        runner.run();

        assertCount(0, 0);
    }

    private void assertCount(int success, int failure) {
        runner.assertTransferCount(UpdateCassandra.REL_FAILURE, failure);
        runner.assertTransferCount(UpdateCassandra.REL_SUCCESS, success);
        runner.clearTransferState();
    }

    @Test
    public void testDeleteFromBody() {
        runner.enqueue("DELETE FROM test_table");
        runner.run();

        assertCount(1, 0 );
    }

    private static class MockDeleteCassandra extends UpdateCassandra {
        private Session mockSession = mock(Session.class);
        private Exception exceptionToThrow = null;

        @Override
        protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                        String username, String password) {
            Cluster mockCluster = mock(Cluster.class);
            try {
                Metadata mockMetadata = mock(Metadata.class);
                when(mockMetadata.getClusterName()).thenReturn("cluster1");
                when(mockCluster.getMetadata()).thenReturn(mockMetadata);
                when(mockCluster.connect()).thenReturn(mockSession);
                when(mockCluster.connect(anyString())).thenReturn(mockSession);
                Configuration config = Configuration.builder().build();
                when(mockCluster.getConfiguration()).thenReturn(config);
                ResultSetFuture future = mock(ResultSetFuture.class);
                ResultSet rs = CassandraQueryTestUtil.createMockResultSet();
                PreparedStatement ps = mock(PreparedStatement.class);
                when(mockSession.prepare(anyString())).thenReturn(ps);
                BoundStatement bs = mock(BoundStatement.class);
                when(ps.bind()).thenReturn(bs);
                when(future.getUninterruptibly()).thenReturn(rs);
                try {
                    doReturn(rs).when(future).getUninterruptibly(anyLong(), any(TimeUnit.class));
                } catch (TimeoutException te) {
                    throw new IllegalArgumentException("Mocked cluster doesn't time out");
                }
                if (exceptionToThrow != null) {
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(anyString());
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(any(Statement.class));

                } else {
                    when(mockSession.executeAsync(anyString())).thenReturn(future);
                    when(mockSession.executeAsync(any(Statement.class))).thenReturn(future);
                }
                when(mockSession.getCluster()).thenReturn(mockCluster);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            return mockCluster;
        }
    }
}
