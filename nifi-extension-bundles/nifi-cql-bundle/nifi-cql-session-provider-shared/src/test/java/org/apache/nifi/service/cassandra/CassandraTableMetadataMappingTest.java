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
package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.apache.nifi.service.cql.api.constants.PrimaryKeyFieldType;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for the driver-metadata-to-{@link PrimaryKey} mapping behind {@code getMetadata}. It walks
 * {@code TableMetadata}'s partition-key list and clustering-column map and records each column's name,
 * position within its own group, and role - no cluster required. The CRUD integration suite used to carry
 * this by reading real tables back; a fabricated {@code TableMetadata} pins the same properties (membership,
 * role, and declaration order) without a container.
 */
class CassandraTableMetadataMappingTest {

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();

    @Test
    @DisplayName("A table with no clustering columns maps to a single PARTITION entry and an empty clustering list")
    void testPartitionKeyOnlyTable() {
        // Column mocks are built into locals first: stubbing one inside the argument of another when(...)
        // trips Mockito's unfinished-stubbing check.
        final ColumnMetadata username = column("username");

        final TableMetadata metadata = mock(TableMetadata.class);
        when(metadata.getPartitionKey()).thenReturn(List.of(username));
        when(metadata.getClusteringColumns()).thenReturn(Map.of());

        final PrimaryKey primaryKey = convertTableMetadata(metadata);

        assertEquals(List.of(new PrimaryKeyMetadata("username", 0, PrimaryKeyFieldType.PARTITION)),
                primaryKey.partitionKey());
        assertTrue(primaryKey.clusteringKeys().isEmpty());
    }

    @Test
    @DisplayName("Clustering columns keep the table's declaration order, each numbered from zero within its group")
    void testClusteringColumnsInDeclarationOrder() {
        final ColumnMetadata sender = column("sender");
        final ColumnMetadata receiver = column("receiver");
        final ColumnMetadata whenSent = column("when_sent");

        final Map<ColumnMetadata, ClusteringOrder> clusteringColumns = new LinkedHashMap<>();
        clusteringColumns.put(receiver, ClusteringOrder.ASC);
        clusteringColumns.put(whenSent, ClusteringOrder.ASC);

        final TableMetadata metadata = mock(TableMetadata.class);
        when(metadata.getPartitionKey()).thenReturn(List.of(sender));
        when(metadata.getClusteringColumns()).thenReturn(clusteringColumns);

        final PrimaryKey primaryKey = convertTableMetadata(metadata);

        assertEquals(List.of(new PrimaryKeyMetadata("sender", 0, PrimaryKeyFieldType.PARTITION)),
                primaryKey.partitionKey());
        assertEquals(List.of(
                        new PrimaryKeyMetadata("receiver", 0, PrimaryKeyFieldType.CLUSTERING),
                        new PrimaryKeyMetadata("when_sent", 1, PrimaryKeyFieldType.CLUSTERING)),
                primaryKey.clusteringKeys());
    }

    private static ColumnMetadata column(final String name) {
        final ColumnMetadata column = mock(ColumnMetadata.class);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal(name));
        return column;
    }

    /**
     * {@code convertTableMetadata} is private and its only caller needs a live session, so it is reached
     * reflectively rather than by widening production visibility purely for a test - the same approach
     * {@link CassandraCQLExecutionServiceWritePathTest} takes.
     */
    private PrimaryKey convertTableMetadata(final TableMetadata metadata) {
        try {
            final Method method = CassandraCQLExecutionService.class.getDeclaredMethod("convertTableMetadata", TableMetadata.class);
            method.setAccessible(true);
            return (PrimaryKey) method.invoke(service, metadata);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            return fail("convertTableMetadata threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke convertTableMetadata", e);
        }
    }
}
