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
package org.apache.nifi.processors.flume;

import java.io.File;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.AvroSource;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeSinkProcessorTest {

  private static final Logger logger =
      LoggerFactory.getLogger(FlumeSinkProcessorTest.class);
  
    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(FlumeSinkProcessor.class);
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
            logger.error(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because Sink Type is required"));
        }

        // non-existent class
        results = new HashSet<>();
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, "invalid.class.name");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            logger.error(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because unable to load sink"));
        }

        // class doesn't implement Sink
        results = new HashSet<>();
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, AvroSource.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            logger.error(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because unable to create sink"));
        }

        results = new HashSet<>();
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, NullSink.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }


    @Test
    public void testNullSink() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(FlumeSinkProcessor.class);
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, NullSink.class.getName());
        FileInputStream fis = new FileInputStream("src/test/resources/testdata/records.txt");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "records.txt");
        runner.enqueue(fis, attributes);
        runner.run();
        fis.close();
    }

    @Test
    public void testBatchSize() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(FlumeSinkProcessor.class);
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, NullSink.class.getName());
        runner.setProperty(FlumeSinkProcessor.BATCH_SIZE, "1000");
        runner.setProperty(FlumeSinkProcessor.FLUME_CONFIG,
            "tier1.sinks.sink-1.batchSize = 1000\n");
        for (int i = 0; i < 100000; i++) {
          runner.enqueue(String.valueOf(i).getBytes());
        }
        runner.run();
    }
    
    @Test
    public void testHdfsSink() throws IOException {
        File destDir = new File("target/hdfs");
        if (destDir.exists()) {
          FileUtils.deleteFilesInDir(destDir, null, logger);
        } else {
          destDir.mkdirs();
        }

        TestRunner runner = TestRunners.newTestRunner(FlumeSinkProcessor.class);
        runner.setProperty(FlumeSinkProcessor.SINK_TYPE, "hdfs");
        runner.setProperty(FlumeSinkProcessor.FLUME_CONFIG,
            "tier1.sinks.sink-1.hdfs.path = " + destDir.toURI().toString() + "\n" +
            "tier1.sinks.sink-1.hdfs.fileType = DataStream\n" +
            "tier1.sinks.sink-1.hdfs.serializer = TEXT\n" +
            "tier1.sinks.sink-1.serializer.appendNewline = false"
        );
        FileInputStream fis = new FileInputStream("src/test/resources/testdata/records.txt");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "records.txt");
        runner.enqueue(fis, attributes);
        runner.run();
        fis.close();

        File[] files = destDir.listFiles((FilenameFilter)HiddenFileFilter.VISIBLE);
        assertEquals("Unexpected number of destination files.", 1, files.length);
        File dst = files[0];
        byte[] expectedMd5 = FileUtils.computeMd5Digest(new File("src/test/resources/testdata/records.txt"));
        byte[] actualMd5 = FileUtils.computeMd5Digest(dst);
        Assert.assertArrayEquals("Destination file doesn't match source data", expectedMd5, actualMd5);
    }
 
}
