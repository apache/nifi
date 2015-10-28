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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.nifi.processors.standard.TailFile.TailFileState;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTailFile {
    private File file;
    private RandomAccessFile raf;
    private TestRunner runner;

    @Before
    public void setup() throws IOException {
        final File targetDir = new File("target");
        final File[] files = targetDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.startsWith("log");
            }
        });

        for (final File file : files) {
            file.delete();
        }

        file = new File("target/log.txt");
        file.delete();
        assertTrue(file.createNewFile());

        final File stateFile = new File("target/tail-file.state");
        stateFile.delete();
        Assert.assertFalse(stateFile.exists());

        runner = TestRunners.newTestRunner(new TailFile());
        runner.setProperty(TailFile.FILENAME, "target/log.txt");
        runner.setProperty(TailFile.STATE_FILE, "target/tail-file.state");
        runner.assertValid();

        raf = new RandomAccessFile(file, "rw");
    }

    @After
    public void cleanup() throws IOException {
        if (raf != null) {
            raf.close();
        }
    }


    @Test
    public void testConsumeAfterTruncationStartAtBeginningOfFile() throws IOException, InterruptedException {
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.txt*");
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE.getValue());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        // roll over the file
        raf.close();
        file.renameTo(new File(file.getParentFile(), file.getName() + ".previous"));
        raf = new RandomAccessFile(file, "rw");

        // truncate file
        raf.setLength(0L);
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        // write some bytes to the file.
        Thread.sleep(1000L); // we need to wait at least one second because of the granularity of timestamps on many file systems.
        raf.write("HELLO\n".getBytes());

        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("HELLO\n");
    }

    @Test
    public void testConsumeAfterTruncationStartAtCurrentTime() throws IOException, InterruptedException {
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_TIME.getValue());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        // truncate and then write same number of bytes
        raf.setLength(0L);
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        Thread.sleep(1000L); // we need to wait at least one second because of the granularity of timestamps on many file systems.
        raf.write("HELLO\n".getBytes());

        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("HELLO\n");
    }


    @Test
    public void testStartAtBeginningOfFile() throws IOException, InterruptedException {
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE.getValue());

        raf.write("hello world\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello world\n");
    }

    @Test
    public void testStartAtCurrentTime() throws IOException, InterruptedException {
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_TIME.getValue());

        raf.write("hello world\n".getBytes());
        runner.run(100);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);
    }

    @Test
    public void testStartAtBeginningOfTime() throws IOException, InterruptedException {
        raf.write("hello".getBytes());
        raf.close();
        file.renameTo(new File(file.getParentFile(), file.getName() + ".previous"));

        raf = new RandomAccessFile(file, "rw");
        raf.write("world\n".getBytes());

        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.txt*");
        runner.setProperty(TailFile.START_POSITION, TailFile.START_BEGINNING_OF_TIME.getValue());

        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);

        boolean world = false;
        boolean hello = false;
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)) {
            final String content = new String(mff.toByteArray());
            if ("world\n".equals(content)) {
                world = true;
            } else if ("hello".equals(content)) {
                hello = true;
            } else {
                Assert.fail("Got unexpected content: " + content);
            }
        }

        assertTrue(hello);
        assertTrue(world);
    }


    @Test
    public void testRemainderOfFileRecoveredAfterRestart() throws IOException {
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log*.txt");
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());
        raf.close();
        file.renameTo(new File("target/log1.txt"));

        raf = new RandomAccessFile(new File("target/log.txt"), "rw");
        raf.write("new file\n".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("new file\n");
    }


    @Test
    public void testRemainderOfFileRecoveredIfRolledOverWhileRunning() throws IOException {
        // this mimics the case when we are reading a log file that rolls over while processor is running.
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log*.txt");
        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());
        raf.close();
        file.renameTo(new File("target/log1.txt"));

        raf = new RandomAccessFile(new File("target/log.txt"), "rw");
        raf.write("1\n".getBytes());
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("1\n");
    }


    @Test
    public void testConsumeWhenNewLineFound() throws IOException, InterruptedException {
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        final long start = System.currentTimeMillis();
        Thread.sleep(1100L);

        raf.write("Hello, World".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("\r\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);

        final TailFileState state = ((TailFile) runner.getProcessor()).getState();
        assertNotNull(state);
        assertEquals("target/log.txt", state.getFilename());
        assertTrue(state.getTimestamp() <= System.currentTimeMillis());
        assertTrue(state.getTimestamp() >= start);
        assertEquals(14, state.getPosition());
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("Hello, World\r\n");

        runner.clearTransferState();

        raf.write("12345".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("12345\n");

        runner.clearTransferState();
        raf.write("carriage\rreturn\r".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("carriage\r");

        runner.clearTransferState();
        raf.write("\r\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("return\r\r\n");

    }

}
