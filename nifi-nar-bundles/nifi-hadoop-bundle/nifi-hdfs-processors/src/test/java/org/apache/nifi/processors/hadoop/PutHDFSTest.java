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
package org.apache.nifi.processors.hadoop;

import org.apache.nifi.processors.hadoop.PutHDFS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class PutHDFSTest {

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(PutHDFS.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

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

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "target");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        runner = TestRunners.newTestRunner(PutHDFS.class);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "0");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        runner = TestRunners.newTestRunner(PutHDFS.class);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [-1] cannot be negative"));
        }

        runner = TestRunners.newTestRunner(PutHDFS.class);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "18");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because [18] is not a valid short octal number"));
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "2000");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [2000] is not a valid umask"));
        }

        results = new HashSet<>();
        runner = TestRunners.newTestRunner(PutHDFS.class);
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, CompressionCodec.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because Given value not found in allowed set"));
        }
    }

    // The following only seems to work from cygwin...something about not finding the 'chmod' command.
    @Ignore
    public void testPutFile() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(PutHDFS.class);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
        runner.enqueue(fis, attributes);
        runner.run();
        fis.close();

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        assertTrue(fs.exists(new Path("target/test-classes/randombytes-1")));
    }
}
