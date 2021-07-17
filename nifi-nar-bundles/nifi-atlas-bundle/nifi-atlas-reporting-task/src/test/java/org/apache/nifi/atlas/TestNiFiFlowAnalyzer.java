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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.reporting.ITReportLineageToAtlas;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.junit.Assert.assertEquals;

/**
 * Test {@link NiFiFlowAnalyzer} with simple mock code.
 * More complex and detailed tests are available in {@link ITReportLineageToAtlas}.
 */
public class TestNiFiFlowAnalyzer {

    private int componentId = 0;

    @Before
    public void before() throws Exception {
        componentId = 0;
    }

    private ProcessGroupStatus createEmptyProcessGroupStatus() {
        final ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();

        processGroupStatus.setId(nextComponentId());
        processGroupStatus.setName("Flow name");

        return processGroupStatus;
    }

    @Test
    public void testEmptyFlow() throws Exception {
        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = new NiFiFlow(rootPG.getId());
        nifiFlow.setNamespace("namespace1");
        analyzer.analyzeProcessGroup(nifiFlow, rootPG);

        assertEquals("1234-5678-0000-0000@namespace1", nifiFlow.getQualifiedName());
    }

    private ProcessorStatus createProcessor(ProcessGroupStatus pgStatus, String type) {
        final ProcessorStatus processor = new ProcessorStatus();
        processor.setName(type);
        processor.setId(nextComponentId());
        processor.setGroupId(pgStatus.getId());
        pgStatus.getProcessorStatus().add(processor);

        return  processor;
    }

    private String nextComponentId() {
        return String.format("1234-5678-0000-%04d", componentId++);
    }

    private void connect(ProcessGroupStatus pg0, Object o0, Object o1) {
        Function<Object, Tuple<String, String>> toTupple = o -> {
            Tuple<String, String> comp;
            if (o instanceof ProcessorStatus) {
                ProcessorStatus p = (ProcessorStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else if (o instanceof PortStatus) {
                PortStatus p = (PortStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else {
                throw new IllegalArgumentException("Not supported");
            }
            return comp;
        };
        connect(pg0, toTupple.apply(o0), toTupple.apply(o1));
    }

    private void connect(ProcessGroupStatus pg0, Tuple<String, String> comp0, Tuple<String, String> comp1) {
        ConnectionStatus conn = new ConnectionStatus();
        conn.setId(nextComponentId());
        conn.setGroupId(pg0.getId());

        conn.setSourceId(comp0.getKey());
        conn.setSourceName(comp0.getValue());

        conn.setDestinationId(comp1.getKey());
        conn.setDestinationName(comp1.getValue());

        pg0.getConnectionStatus().add(conn);
    }

    @Test
    public void testSingleProcessor() throws Exception {

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = new NiFiFlow(rootPG.getId());
        analyzer.analyzeProcessGroup(nifiFlow, rootPG);

        assertEquals(1, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final Map<String, NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        // first path
        final NiFiFlowPath path0 = paths.get(pr0.getId());
        assertEquals(path0.getId(), path0.getProcessComponentIds().get(0));
        assertEquals(rootPG.getId(), path0.getGroupId());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        assertEquals(path0, pathForPr0);
    }


    @Test
    public void testProcessorsWithinSinglePath() throws Exception {

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");

        connect(rootPG, pr0, pr1);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = new NiFiFlow(rootPG.getId());
        analyzer.analyzeProcessGroup(nifiFlow, rootPG);

        assertEquals(2, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final Map<String, NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath path0 = paths.get(pr0.getId());
        assertEquals(path0, pathForPr0);
        assertEquals(path0, pathForPr1);
    }

    @Test
    public void testMultiPaths() throws Exception {

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "LogAttribute");

        connect(rootPG, pr0, pr1);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = new NiFiFlow(rootPG.getId());
        analyzer.analyzeProcessGroup(nifiFlow, rootPG);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final Map<String, NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());

        // Order is not guaranteed
        final NiFiFlowPath pathA = paths.get(pr0.getId());
        final NiFiFlowPath pathB = paths.get(pr2.getId());
        assertEquals(2, pathA.getProcessComponentIds().size());
        assertEquals(2, pathB.getProcessComponentIds().size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathB, pathForPr3);
    }

    @Test
    public void testMultiPathsJoint() throws Exception {

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        final ProcessorStatus pr0 = createProcessor(rootPG, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "org.apache.nifi.processors.standard.ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "org.apache.nifi.processors.standard.LogAttribute");

        // Result should be as follows:
        // pathA = 0 -> 1 (-> 3)
        // pathB = 2 (-> 3)
        // pathC = 3
        connect(rootPG, pr0, pr1);
        connect(rootPG, pr1, pr3);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = new NiFiFlow(rootPG.getId());
        nifiFlow.setNamespace("namespace1");
        analyzer.analyzeProcessGroup(nifiFlow, rootPG);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final Map<String, NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(3, paths.size());

        // Order is not guaranteed
        final NiFiFlowPath pathA = paths.get(pr0.getId());
        final NiFiFlowPath pathB = paths.get(pr2.getId());
        final NiFiFlowPath pathC = paths.get(pr3.getId());
        assertEquals(2, pathA.getProcessComponentIds().size());
        assertEquals(1, pathB.getProcessComponentIds().size());
        assertEquals(1, pathC.getProcessComponentIds().size());

        // A queue is added as input for the joint point.
        assertEquals(1, pathC.getInputs().size());
        final AtlasObjectId queue = pathC.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_QUEUE, queue.getTypeName());
        assertEquals(toQualifiedName("namespace1", pathC.getId()), queue.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathC, pathForPr3);
    }

}
