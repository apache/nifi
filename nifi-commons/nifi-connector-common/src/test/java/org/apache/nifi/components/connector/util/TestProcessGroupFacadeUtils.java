/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.components.connector.util;

import org.apache.nifi.components.connector.components.ComponentHierarchyScope;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestProcessGroupFacadeUtils {

    private static final String MERGE_TYPE = "com.example.MergeSnowflakeJournalTable";
    private static final String FETCH_SCHEMA_TYPE = "com.example.FetchSourceTableSchema";
    private static final String LIST_TABLES_TYPE = "com.example.ListTableNames";
    private static final String DBCP_TYPE = "org.apache.nifi.dbcp.DBCPConnectionPool";
    private static final String NESTED_DBCP_TYPE = "org.apache.nifi.dbcp.HikariCPConnectionPool";

    @Nested
    class FindProcessors {
        @Test
        void testRecursiveSearchIncludesNestedProcessor() {
            final ProcessorFacade nested = processor("Fetch Source Table Schema", FETCH_SCHEMA_TYPE);
            final ProcessGroupFacade performSnapshot = processGroup("Perform Snapshot Load", Set.of(nested), Set.of(), Set.of());
            final ProcessGroupFacade snapshotLoad = processGroup("Snapshot Load", Set.of(), Set.of(performSnapshot), Set.of());
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(snapshotLoad), Set.of());

            final List<ProcessorFacade> found = ProcessGroupFacadeUtils.findProcessors(root, ProcessGroupFacadeUtils.processorTypeEndsWith("FetchSourceTableSchema"));

            assertEquals(List.of(nested), found);
        }

        @Test
        void testImmediateSearchExcludesNestedProcessor() {
            final ProcessorFacade nested = processor("Fetch Source Table Schema", FETCH_SCHEMA_TYPE);
            final ProcessGroupFacade child = processGroup("Child", Set.of(nested), Set.of(), Set.of());
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(child), Set.of());

            final List<ProcessorFacade> found = ProcessGroupFacadeUtils.findProcessors(root, processor -> true, ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY);

            assertTrue(found.isEmpty());
        }

        @Test
        void testFindProcessorRequiresExactlyOneMatch() {
            final ProcessorFacade merge = processor("Merge Journal to Destination", MERGE_TYPE);
            final ProcessorFacade otherMerge = processor("Merge Journal History", MERGE_TYPE);
            final ProcessGroupFacade single = processGroup("Root", Set.of(merge), Set.of(), Set.of());
            final ProcessGroupFacade duplicate = processGroup("Root", Set.of(merge, otherMerge), Set.of(), Set.of());
            final ProcessGroupFacade empty = processGroup("Root", Set.of(), Set.of(), Set.of());

            assertEquals(Optional.of(merge), ProcessGroupFacadeUtils.findProcessor(single, ProcessGroupFacadeUtils.processorTypeEndsWith("MergeSnowflakeJournalTable")));
            assertTrue(ProcessGroupFacadeUtils.findProcessor(duplicate, ProcessGroupFacadeUtils.processorTypeEndsWith("MergeSnowflakeJournalTable")).isEmpty());
            assertTrue(ProcessGroupFacadeUtils.findProcessor(empty, ProcessGroupFacadeUtils.processorTypeEndsWith("MergeSnowflakeJournalTable")).isEmpty());
        }
    }

    @Nested
    class FindInGroupPath {
        @Test
        void testFindsImmediateProcessorAtPath() {
            final ProcessorFacade listTables = processor("List Table Names", LIST_TABLES_TYPE);
            final ProcessorFacade nestedFetch = processor("Fetch Source Table Schema", FETCH_SCHEMA_TYPE);
            final ProcessGroupFacade performSnapshot = processGroup("Perform Snapshot Load", Set.of(nestedFetch), Set.of(), Set.of());
            final ProcessGroupFacade snapshotLoad = processGroup("Snapshot Load", Set.of(listTables), Set.of(performSnapshot), Set.of());
            final ProcessGroupFacade incrementalLoad = processGroup("Incremental Load", Set.of(processor("Merge Journal to Destination", MERGE_TYPE)), Set.of(), Set.of());
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(snapshotLoad, incrementalLoad), Set.of());

            final Optional<ProcessorFacade> listTableNames = ProcessGroupFacadeUtils.findProcessorInGroupPath(root,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("ListTableNames"), "Snapshot Load");
            final Optional<ProcessorFacade> fetchSchema = ProcessGroupFacadeUtils.findProcessorInGroupPath(root,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("FetchSourceTableSchema"), "Snapshot Load", "Perform Snapshot Load");
            final Optional<ProcessorFacade> mergeInSnapshot = ProcessGroupFacadeUtils.findProcessorInGroupPath(root,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("MergeSnowflakeJournalTable"), "Snapshot Load");

            assertEquals(Optional.of(listTables), listTableNames);
            assertEquals(Optional.of(nestedFetch), fetchSchema);
            assertTrue(mergeInSnapshot.isEmpty());
        }

        @Test
        void testDoesNotRecurseFromTerminalGroup() {
            final ProcessorFacade nestedFetch = processor("Fetch Source Table Schema", FETCH_SCHEMA_TYPE);
            final ProcessGroupFacade performSnapshot = processGroup("Perform Snapshot Load", Set.of(nestedFetch), Set.of(), Set.of());
            final ProcessGroupFacade snapshotLoad = processGroup("Snapshot Load", Set.of(), Set.of(performSnapshot), Set.of());
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(snapshotLoad), Set.of());

            final Optional<ProcessorFacade> found = ProcessGroupFacadeUtils.findProcessorInGroupPath(root,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("FetchSourceTableSchema"), "Snapshot Load");

            assertTrue(found.isEmpty());
        }

        @Test
        void testMissingOrDuplicatePathSegmentReturnsEmpty() {
            final ProcessorFacade merge = processor("Merge Journal to Destination", MERGE_TYPE);
            final ProcessGroupFacade incrementalA = processGroup("Incremental Load", Set.of(merge), Set.of(), Set.of());
            final ProcessGroupFacade incrementalB = processGroup("Incremental Load", Set.of(), Set.of(), Set.of());
            final ProcessGroupFacade duplicateRoot = processGroup("Root", Set.of(), Set.of(incrementalA, incrementalB), Set.of());
            final ProcessGroupFacade missingRoot = processGroup("Root", Set.of(), Set.of(), Set.of());

            assertTrue(ProcessGroupFacadeUtils.findProcessorInGroupPath(duplicateRoot,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("MergeSnowflakeJournalTable"), "Incremental Load").isEmpty());
            assertTrue(ProcessGroupFacadeUtils.findProcessGroupByNamePath(missingRoot, "Incremental Load").isEmpty());
        }

        @Test
        void testEmptyPathSearchesImmediateRoot() {
            final ProcessorFacade generate = processor("Generate", "org.apache.nifi.processors.standard.GenerateFlowFile");
            final ProcessGroupFacade root = processGroup("Root", Set.of(generate), Set.of(), Set.of());

            assertEquals(Optional.of(generate), ProcessGroupFacadeUtils.findProcessorInGroupPath(root,
                    ProcessGroupFacadeUtils.processorTypeEndsWith("GenerateFlowFile")));
        }
    }

    @Nested
    class FindControllerServices {
        @Test
        void testRecursiveAndImmediateScopes() {
            final ControllerServiceFacade nested = controllerService("Nested DBCP", NESTED_DBCP_TYPE);
            final ControllerServiceFacade rootService = controllerService("DBCP", DBCP_TYPE);
            final ProcessGroupFacade child = processGroup("Child", Set.of(), Set.of(), Set.of(nested));
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(child), Set.of(rootService));

            assertEquals(List.of(rootService, nested), ProcessGroupFacadeUtils.findControllerServices(root, service -> true));
            assertEquals(List.of(rootService), ProcessGroupFacadeUtils.findControllerServices(root, service -> true, ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
            assertEquals(Optional.of(rootService), ProcessGroupFacadeUtils.findControllerService(root, ProcessGroupFacadeUtils.controllerServiceTypeEndsWith("DBCPConnectionPool"),
                    ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
        }
    }

    @Nested
    class FindProcessGroups {
        @Test
        void testFindsNestedGroupByName() {
            final ProcessGroupFacade performSnapshot = processGroup("Perform Snapshot Load", Set.of(), Set.of(), Set.of());
            final ProcessGroupFacade snapshotLoad = processGroup("Snapshot Load", Set.of(), Set.of(performSnapshot), Set.of());
            final ProcessGroupFacade root = processGroup("Root", Set.of(), Set.of(snapshotLoad), Set.of());

            assertEquals(Optional.of(performSnapshot), ProcessGroupFacadeUtils.findProcessGroup(root, ProcessGroupFacadeUtils.processGroupNamed("Perform Snapshot Load")));
            assertEquals(Optional.of(performSnapshot), ProcessGroupFacadeUtils.findProcessGroupByNamePath(root, "Snapshot Load", "Perform Snapshot Load"));
            assertEquals(List.of(snapshotLoad), ProcessGroupFacadeUtils.findProcessGroups(root, ProcessGroupFacadeUtils.processGroupNamed("Snapshot Load"),
                    ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
        }
    }

    private static ProcessGroupFacade processGroup(final String name, final Set<ProcessorFacade> processors, final Set<ProcessGroupFacade> children,
            final Set<ControllerServiceFacade> services) {
        final ProcessGroupFacade group = mock(ProcessGroupFacade.class);
        final VersionedProcessGroup definition = mock(VersionedProcessGroup.class);
        when(definition.getName()).thenReturn(name);
        when(group.getDefinition()).thenReturn(definition);
        when(group.getProcessors()).thenReturn(processors);
        when(group.getProcessGroups()).thenReturn(children);
        when(group.getControllerServices()).thenReturn(services);
        return group;
    }

    private static ProcessorFacade processor(final String name, final String type) {
        final ProcessorFacade processor = mock(ProcessorFacade.class);
        final VersionedProcessor definition = mock(VersionedProcessor.class);
        when(definition.getName()).thenReturn(name);
        when(definition.getType()).thenReturn(type);
        when(processor.getDefinition()).thenReturn(definition);
        return processor;
    }

    private static ControllerServiceFacade controllerService(final String name, final String type) {
        final ControllerServiceFacade service = mock(ControllerServiceFacade.class);
        final VersionedControllerService definition = mock(VersionedControllerService.class);
        when(definition.getName()).thenReturn(name);
        when(definition.getType()).thenReturn(type);
        when(service.getDefinition()).thenReturn(definition);
        return service;
    }
}
