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
package org.apache.nifi.fn.core;

public class RegistryTest {

    @org.junit.Test
    public void testScenario1_Test() throws Exception {

        /*VariableRegistry variableRegistry = () -> {
            Map<VariableDescriptor,String> map = new HashMap<>();
            map.put(new VariableDescriptor("SourceCluster"),"hdfs://172.16.96.132:8020");
            map.put(new VariableDescriptor("SourceDirectory"),"hdfs://172.16.96.132:8020/tmp/nififn/input");
            map.put(new VariableDescriptor("SourceFile"),"test.txt");
            map.put(new VariableDescriptor("DestinationDirectory"),"hdfs://172.16.96.132:8020/tmp/nififn/output");
            return map;
        };
        FnFlow flow = new FnFlow("http://172.16.96.132:61080","bucket1","HDFS_to_HDFS", variableRegistry);

        flow.runOnce();

        String outputFile = "/tmp/nififn/output2/test2.txt";
        assertTrue(new File(outputFile).isFile());

        List<String> lines = Files.readAllLines(Paths.get(outputFile), StandardCharsets.UTF_8);

        assertEquals(1,lines.size());
        assertEquals("hello world2", lines.get(0));*/
    }
}
