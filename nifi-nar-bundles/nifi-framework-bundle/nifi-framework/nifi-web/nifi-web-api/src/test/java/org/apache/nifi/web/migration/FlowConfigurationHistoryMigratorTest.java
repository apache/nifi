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
package org.apache.nifi.web.migration;

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FlowConfigurationHistoryMigratorTest {
    private static final String FLOW_AUDIT_DATABASE = "/migration/nifi-flow-audit.mv.db";

    @Mock
    private AuditService auditService;

    @Captor
    private ArgumentCaptor<Collection<Action>> actionsCaptor;

    @Test
    void testReconcileDatabaseRepositoryMigrationRequired(@TempDir final Path tempDir) throws Exception {
        final URL sourceFlowAuditDatabaseUrl = Objects.requireNonNull(getClass().getResource(FLOW_AUDIT_DATABASE), "Database not found");
        final Path sourceFlowAuditDatabasePath = Paths.get(sourceFlowAuditDatabaseUrl.toURI());
        final Path flowAuditDatabasePath = tempDir.resolve(sourceFlowAuditDatabasePath.getFileName());
        Files.copy(sourceFlowAuditDatabasePath, flowAuditDatabasePath);

        FlowConfigurationHistoryMigrator.reconcileDatabaseRepository(tempDir, auditService);

        verify(auditService).addActions(actionsCaptor.capture());
        final Collection<Action> actions = actionsCaptor.getValue();
        assertNotNull(actions);
        assertEquals(2, actions.size());
    }
}
