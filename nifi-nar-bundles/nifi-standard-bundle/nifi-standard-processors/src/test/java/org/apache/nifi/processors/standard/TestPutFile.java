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
package org.apache.nifi.processors.standard;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPutFile {

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(PutFile.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // test invalid directory attribute not set
        results = new HashSet<>();
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Directory is required"));
        }

        // test invalid directory attribute set
        runner = TestRunners.newTestRunner(PutFile.class);
        results = new HashSet<>();
        runner.setProperty(PutFile.DIRECTORY, "target");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        // test invalid FILENAME_EXPRESSION
        runner = TestRunners.newTestRunner(PutFile.class);
        results = new HashSet<>();
        runner.setProperty(PutFile.DIRECTORY, "target");
        runner.setProperty(PutFile.FILENAME_EXPRESSION, "target/data_${literal('testing'):foo()}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("validated against 'target/data_${literal('testing'):foo()}' is invalid"));
        }

        // test valid FILENAME_EXPRESSION
        runner = TestRunners.newTestRunner(PutFile.class);
        results = new HashSet<>();
        runner.setProperty(PutFile.DIRECTORY, "target");
        runner.setProperty(PutFile.FILENAME_EXPRESSION, "${filename:toLower()}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());
    }

    @Test
    public void testPutFile() throws IOException {

        TestRunner runner = TestRunners.newTestRunner(PutFile.class);
        runner.setProperty(PutFile.DIRECTORY, "target/test-classes");
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        ;

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutFile.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        File file = new File ("target/test-classes/randombytes-1");
        assertTrue(file.exists());
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    }

    @Test
    public void testPutFileWithException() throws IOException {

        String dirName = "target/testPutFileWrongPermissions";
        File file = new File(dirName);
        file.mkdirs();

        // Path p = new Path(dirName).makeQualified(fs.getUri(), fs.getWorkingDirectory());


        TestRunner runner = TestRunners.newTestRunner(new PutFile() {
            @Override
            protected void changeOwner(ProcessContext context, FlowFile flowFile, Path name, ComponentLog logger) {
                throw new ProcessException("Forcing Exception to get thrown in order to verify proper handling");
            }

        });
        runner.setProperty(PutFile.DIRECTORY, dirName);
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertFalse(failedFlowFiles.isEmpty());
        assertTrue(failedFlowFiles.get(0).isPenalized());

        // fs.delete(p, true);
    }



}
