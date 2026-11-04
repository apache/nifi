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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectionSettingsChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String CHILD_GROUP = "child-group";
    private static final String CONN_A = "conn-a";
    private static final String CONN_B = "conn-b";
    private static final String FIFO_PRIORITIZER = "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer";
    private static final String NEWEST_PRIORITIZER = "org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer";

    private static final Map<DifferenceType, Setting> SETTINGS = Map.of(
            DifferenceType.BACKPRESSURE_OBJECT_THRESHOLD_CHANGED, new Setting(10000L, 500L, 2000L,
                    VersionedConnection::getBackPressureObjectThreshold, (connection, value) -> connection.setBackPressureObjectThreshold((Long) value)),
            DifferenceType.BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED, new Setting("1 GB", "500 MB", "2 GB",
                    VersionedConnection::getBackPressureDataSizeThreshold, (connection, value) -> connection.setBackPressureDataSizeThreshold((String) value)),
            DifferenceType.FLOWFILE_EXPIRATION_CHANGED, new Setting("0 sec", "5 min", "10 min",
                    VersionedConnection::getFlowFileExpiration, (connection, value) -> connection.setFlowFileExpiration((String) value)),
            DifferenceType.PRIORITIZERS_CHANGED, new Setting(List.of(), List.of(FIFO_PRIORITIZER), List.of(NEWEST_PRIORITIZER),
                    VersionedConnection::getPrioritizers, (connection, value) -> connection.setPrioritizers(toStringList(value))));

    @ParameterizedTest
    @EnumSource(value = DifferenceType.class, names = {"BACKPRESSURE_OBJECT_THRESHOLD_CHANGED", "BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED",
            "FLOWFILE_EXPIRATION_CHANGED", "PRIORITIZERS_CHANGED"})
    void testNoUpstreamChange(final DifferenceType type) {
        final Setting setting = SETTINGS.get(type);
        final ConnectionSettingsChangedRebaseHandler handler = new ConnectionSettingsChangedRebaseHandler(type);
        final FlowDifference localDifference = createDifference(type, CONN_A, setting.local());
        final FlowDifference upstreamOtherConnection = createDifference(type, CONN_B, setting.upstream());

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamOtherConnection), createGroup(ROOT));

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @ParameterizedTest
    @EnumSource(value = DifferenceType.class, names = {"BACKPRESSURE_OBJECT_THRESHOLD_CHANGED", "BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED",
            "FLOWFILE_EXPIRATION_CHANGED", "PRIORITIZERS_CHANGED"})
    void testUpstreamChangedToSameValue(final DifferenceType type) {
        final Setting setting = SETTINGS.get(type);
        final ConnectionSettingsChangedRebaseHandler handler = new ConnectionSettingsChangedRebaseHandler(type);
        final FlowDifference localDifference = createDifference(type, CONN_A, setting.local());
        final FlowDifference upstreamDifference = createDifference(type, CONN_A, setting.local());

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createGroup(ROOT));

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @ParameterizedTest
    @EnumSource(value = DifferenceType.class, names = {"BACKPRESSURE_OBJECT_THRESHOLD_CHANGED", "BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED",
            "FLOWFILE_EXPIRATION_CHANGED", "PRIORITIZERS_CHANGED"})
    void testUpstreamChangedToDifferentValue(final DifferenceType type) {
        final Setting setting = SETTINGS.get(type);
        final ConnectionSettingsChangedRebaseHandler handler = new ConnectionSettingsChangedRebaseHandler(type);
        final FlowDifference localDifference = createDifference(type, CONN_A, setting.local());
        final FlowDifference upstreamDifference = createDifference(type, CONN_A, setting.upstream());

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createGroup(ROOT));

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_CONNECTION_SETTING, result.getConflictCode());
    }

    @ParameterizedTest
    @EnumSource(value = DifferenceType.class, names = {"BACKPRESSURE_OBJECT_THRESHOLD_CHANGED", "BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED",
            "FLOWFILE_EXPIRATION_CHANGED", "PRIORITIZERS_CHANGED"})
    void testApplyConnectionInChildGroup(final DifferenceType type) {
        final Setting setting = SETTINGS.get(type);
        final ConnectionSettingsChangedRebaseHandler handler = new ConnectionSettingsChangedRebaseHandler(type);
        final VersionedProcessGroup mergedFlow = createGroup(ROOT);
        final VersionedProcessGroup childGroup = createGroup(CHILD_GROUP);
        mergedFlow.getProcessGroups().add(childGroup);
        final VersionedConnection connection = createConnection(CONN_A, setting, setting.original());
        childGroup.getConnections().add(connection);

        final FlowDifference localDifference = createDifference(type, CONN_A, setting.local());
        assertEquals(RebaseClassification.COMPATIBLE, handler.classify(localDifference, Collections.emptySet(), mergedFlow).getClassification());
        handler.apply(localDifference, mergedFlow);

        assertEquals(setting.local(), setting.getter().apply(connection));
    }

    @Test
    void testUnsupportedDifferenceTypeRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionSettingsChangedRebaseHandler(DifferenceType.LOAD_BALANCE_STRATEGY_CHANGED));
    }

    private FlowDifference createDifference(final DifferenceType type, final String identifier, final Object localValue) {
        final Setting setting = SETTINGS.get(type);
        return new StandardFlowDifference(type, createConnection(identifier, setting, setting.original()), createConnection(identifier, setting, localValue),
                setting.original(), localValue, "Connection setting changed");
    }

    private VersionedConnection createConnection(final String identifier, final Setting setting, final Object value) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(identifier);
        setting.setter().accept(connection, value);
        return connection;
    }

    private VersionedProcessGroup createGroup(final String identifier) {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setIdentifier(identifier);
        return group;
    }

    private static List<String> toStringList(final Object value) {
        return ((List<?>) value).stream().map(String.class::cast).toList();
    }

    private record Setting(Object original, Object local, Object upstream, Function<VersionedConnection, Object> getter,
                           BiConsumer<VersionedConnection, Object> setter) {
    }
}
