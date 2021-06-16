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
package org.apache.nifi.stateless.core;

import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.standard.GetFile;
import org.apache.nifi.processors.standard.PutFile;
import org.apache.nifi.processors.standard.ReplaceText;
import org.apache.nifi.processors.standard.SplitText;
import org.apache.nifi.registry.VariableRegistry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchTest {

    @org.junit.Test
    public void testScenario1_Test() throws Exception {

        ///////////////////////////////////////////
        // Setup
        ///////////////////////////////////////////
        VariableRegistry registry = VariableRegistry.EMPTY_REGISTRY;
        boolean materializeData = true;
        StatelessControllerServiceLookup serviceLookup = new StatelessControllerServiceLookup(null);
        File file = new File("/tmp/nifistateless/input/test.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        try (PrintStream out = new PrintStream(new FileOutputStream(file))) {
            out.print("hello world");
        }

        ///////////////////////////////////////////
        // Build Flow
        ///////////////////////////////////////////
        StatelessProcessorWrapper getFile = new StatelessProcessorWrapper(UUID.randomUUID().toString(), new GetFile(), null, serviceLookup, registry,
            materializeData, ClassLoader.getSystemClassLoader(), null);
        getFile.setProperty(GetFile.DIRECTORY,"/tmp/nifistateless/input/");
        getFile.setProperty(GetFile.FILE_FILTER,"test.txt");
        getFile.setProperty(GetFile.KEEP_SOURCE_FILE,"true");

        StatelessProcessorWrapper splitText = wrapProcessor(new SplitText(), serviceLookup, registry);
        getFile.addChild(splitText, GetFile.REL_SUCCESS);
        splitText.setProperty(SplitText.LINE_SPLIT_COUNT,"1");
        splitText.addAutoTermination(SplitText.REL_FAILURE);
        splitText.addAutoTermination(SplitText.REL_ORIGINAL);

        StatelessProcessorWrapper replaceText = wrapProcessor(new ReplaceText(), serviceLookup, registry);
        splitText.addChild(replaceText, SplitText.REL_SPLITS);
        replaceText.setProperty(ReplaceText.REPLACEMENT_VALUE,"$1!!!");
        replaceText.addAutoTermination(ReplaceText.REL_FAILURE);

        StatelessProcessorWrapper putFile = wrapProcessor(new PutFile(), serviceLookup, registry);
        replaceText.addChild(putFile, ReplaceText.REL_SUCCESS);
        putFile.addAutoTermination(PutFile.REL_FAILURE);
        putFile.addAutoTermination(PutFile.REL_SUCCESS);
        putFile.setProperty(PutFile.DIRECTORY,"/tmp/nifistateless/output");
        putFile.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        ///////////////////////////////////////////
        // Run Flow
        ///////////////////////////////////////////
        StatelessFlow flow = new StatelessFlow(getFile);

        Queue<InMemoryFlowFile> output = new LinkedList<>();
        boolean successful = flow.runOnce(output);

        ///////////////////////////////////////////
        // Validate
        ///////////////////////////////////////////
        String outputFile = "/tmp/nifistateless/output/test.txt";
        assertTrue(new File(outputFile).isFile());

        List<String> lines = Files.readAllLines(Paths.get(outputFile), StandardCharsets.UTF_8);

        assertTrue(successful);
        assertTrue(output.isEmpty());
        assertEquals(1,lines.size());
        assertEquals("hello world!!!", lines.get(0));
    }

    private StatelessProcessorWrapper wrapProcessor(final Processor processor, StatelessControllerServiceLookup serviceLookup, final VariableRegistry registry)
                throws InvocationTargetException, IllegalAccessException {
        return new StatelessProcessorWrapper(UUID.randomUUID().toString(), processor, null, serviceLookup, registry, true, ClassLoader.getSystemClassLoader(), null);
    }
}
