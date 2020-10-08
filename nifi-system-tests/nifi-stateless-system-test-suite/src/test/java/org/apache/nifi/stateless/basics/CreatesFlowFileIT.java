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

package org.apache.nifi.stateless.basics;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CreatesFlowFileIT extends StatelessSystemIT {

    @Test
    public void testFlowFileCreated() throws IOException, StatelessConfigurationException {
        final StatelessDataflow dataflow = loadDataflow(new File("src/test/resources/flows/GenerateFlowFile.json"), Collections.emptyList());
        dataflow.trigger();

        assertEquals(Collections.singleton("Out"), dataflow.getOutputPortNames());

        final List<FlowFile> flowFiles = dataflow.drainOutputQueues("Out");
        assertEquals(1, flowFiles.size());

        final FlowFile flowFile = flowFiles.get(0);
        assertEquals("hello", flowFile.getAttribute("greeting"));

        final byte[] bytes = dataflow.getFlowFileContents(flowFile);
        assertEquals("Hello", new String(bytes, StandardCharsets.UTF_8));
    }
}
