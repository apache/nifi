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
    private TailFile processor;
    private RandomAccessFile raf;
    private TestRunner runner;

    @Before
    public void setup() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "TRACE");

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

        processor = new TailFile();
        runner = TestRunners.newTestRunner(processor);
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

        processor.cleanup();
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
        System.out.println("Ingested 6 bytes");
        runner.clearTransferState();

        // roll over the file
        raf.close();
        file.renameTo(new File(file.getParentFile(), file.getName() + ".previous"));
        raf = new RandomAccessFile(file, "rw");
        System.out.println("Rolled over file to " + file.getName() + ".previous");

        // truncate file
        raf.setLength(0L);
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        // write some bytes to the file.
        Thread.sleep(1000L); // we need to wait at least one second because of the granularity of timestamps on many file systems.
        raf.write("HELLO\n".getBytes());

        System.out.println("Wrote out 6 bytes to tailed file");
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("HELLO\n");
    }

    @Test
    public void testConsumeAfterTruncationStartAtCurrentTime() throws IOException, InterruptedException {
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_TIME.getValue());
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.txt*");
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        // truncate and then write same number of bytes
        raf.close();
        assertTrue(file.renameTo(new File("target/log.txt.1")));
        raf = new RandomAccessFile(file, "rw");

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
        Thread.sleep(1000L);
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
        runner.run(1, false, true);
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
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("1\n");
    }

    @Test
    public void testRolloverAfterHavingReadAllData() throws IOException, InterruptedException {
        // If we have read all data in a file, and that file does not end with a new-line, then the last line
        // in the file will have been read, added to the checksum, and then we would re-seek to "unread" that
        // last line since it didn't have a new-line. We need to ensure that if the data is then rolled over
        // that our checksum does not take into account those bytes that have been "unread."

        // this mimics the case when we are reading a log file that rolls over while processor is running.
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.*");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());

        Thread.sleep(1000L);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0); // should not pull in data because no \n

        raf.close();
        file.renameTo(new File("target/log.1"));

        raf = new RandomAccessFile(new File("target/log.txt"), "rw");
        raf.write("1\n".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("1\n");
    }


    @Test
    public void testMultipleRolloversAfterHavingReadAllData() throws IOException, InterruptedException {
        // this mimics the case when we are reading a log file that rolls over while processor is running.
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.*");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());
        runner.run(1); // ensure that we've read 'world' but not consumed it into a flowfile.

        Thread.sleep(1000L);

        // rename file to log.2
        raf.close();
        file.renameTo(new File("target/log.2"));

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("abc\n".getBytes());

        Thread.sleep(100L);

        // rename file to log.1
        raf.close();
        file.renameTo(new File("target/log.1"));

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("1\n".getBytes());
        raf.close();

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 3);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("abc\n");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(2).assertContentEquals("1\n");
    }


    @Test
    public void testMultipleRolloversAfterHavingReadAllDataWhileStillRunning() throws IOException, InterruptedException {
        // this mimics the case when we are reading a log file that rolls over while processor is running.
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.*");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());
        runner.run(1, false, false); // ensure that we've read 'world' but not consumed it into a flowfile.

        Thread.sleep(1000L);

        // rename file to log.2
        raf.close();
        file.renameTo(new File("target/log.2"));

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("abc\n".getBytes());

        Thread.sleep(100L);

        // rename file to log.1
        raf.close();
        file.renameTo(new File("target/log.1"));

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("1\n".getBytes());
        raf.close();

        runner.run(1, true, false); // perform shutdown but do not perform initialization because last iteration didn't shutdown.

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 3);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("abc\n");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(2).assertContentEquals("1\n");
    }


    @Test
    public void testMultipleRolloversWithLongerFileLength() throws IOException, InterruptedException {
        // this mimics the case when we are reading a log file that rolls over while processor is running.
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.*");
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        raf.write("hello\n".getBytes());
        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("hello\n");
        runner.clearTransferState();

        raf.write("world".getBytes());

        // rename file to log.2
        raf.close();
        file.renameTo(new File("target/log.2"));

        Thread.sleep(1200L);

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("abc\n".getBytes());

        // rename file to log.1
        raf.close();
        file.renameTo(new File("target/log.1"));
        Thread.sleep(1200L);

        // write to a new file.
        file = new File("target/log.txt");
        raf = new RandomAccessFile(file, "rw");
        raf.write("This is a longer line than the other files had.\n".getBytes());
        raf.close();

        runner.run(1, true, false); // perform shutdown but do not perform initialization because last iteration didn't shutdown.

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 3);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("world");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(1).assertContentEquals("abc\n");
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(2).assertContentEquals("This is a longer line than the other files had.\n");
    }


    @Test
    public void testConsumeWhenNewLineFound() throws IOException, InterruptedException {
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

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

    @Test
    public void testRolloverAndUpdateAtSameTime() throws IOException {
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "log.*");

        // write out some data and ingest it.
        raf.write("hello there\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.clearTransferState();

        // roll the file over and write data to the new log.txt file.
        raf.write("another".getBytes());
        raf.close();
        file.renameTo(new File("target/log.1"));
        raf = new RandomAccessFile(file, "rw");
        raf.write("new file\n".getBytes());

        // Run the processor. We should get 2 files because we should get the rest of what was
        // written to log.txt before it rolled, and then we should get some data from the new log.txt.
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("another");

        // If we run again, we should get nothing.
        // We did have an issue where we were recognizing the previously rolled over file again because the timestamps
        // were still the same (second-level precision on many file systems). As a result, we verified the checksum of the
        // already-rolled file against the checksum of the new file and they didn't match, so we ingested the entire rolled
        // file as well as the new file again. Instead, we should ingest nothing!
        runner.clearTransferState();
        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);
    }

}
