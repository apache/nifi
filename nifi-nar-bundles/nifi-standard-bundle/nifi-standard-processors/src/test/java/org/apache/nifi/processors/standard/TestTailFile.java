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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processors.standard.TailFile.TailFileState;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assume.assumeFalse;
import org.junit.Before;
import org.junit.Test;

public class TestTailFile {

    private File file;
    private File existingFile;
    private File otherFile;

    private RandomAccessFile raf;
    private RandomAccessFile otherRaf;

    private TailFile processor;
    private TestRunner runner;

    @Before
    public void setup() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "TRACE");
        clean();

        file = new File("target/log.txt");
        file.delete();
        assertTrue(file.createNewFile());

        existingFile = new File("target/existing-log.txt");
        existingFile.delete();
        assertTrue(existingFile.createNewFile());
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(existingFile)))) {
            writer.write("Line 1");
            writer.newLine();
            writer.write("Line 2");
            writer.newLine();
            writer.write("Line 3");
            writer.newLine();
            writer.flush();
        }

        File directory = new File("target/testDir");
        if(!directory.exists()) {
            assertTrue(directory.mkdirs());
        }
        otherFile = new File("target/testDir/log.txt");
        otherFile.delete();
        assertTrue(otherFile.createNewFile());

        processor = new TailFile();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TailFile.FILENAME, "target/log.txt");
        runner.assertValid();

        raf = new RandomAccessFile(file, "rw");
        otherRaf = new RandomAccessFile(otherFile, "rw");
    }

    @After
    public void cleanup() throws IOException {
        if (raf != null) {
            raf.close();
        }

        if (otherRaf != null) {
            otherRaf.close();
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

        final TailFileState state = ((TailFile) runner.getProcessor()).getState().get("target/log.txt").getState();
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

    @Test
    public void testRolloverWhenNoRollingPattern() throws IOException {
        // write out some data and ingest it.
        raf.write("hello there\n".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.clearTransferState();

        // move the file and write data to the new log.txt file.
        raf.write("another".getBytes());
        raf.close();
        file.renameTo(new File("target/log.1"));
        raf = new RandomAccessFile(file, "rw");
        raf.write("new file\n".getBytes());

        // because the roll over pattern has not been set we are not able to get
        // data before the file has been moved, but we still want to ingest data
        // from the tailed file
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("new file\n");
        runner.clearTransferState();

        // in the unlikely case where more data is written after the file is moved
        // we are not able to detect it is a completely new file, then we continue
        // on the tailed file as it never changed
        raf.close();
        file.renameTo(new File("target/log.2"));
        raf = new RandomAccessFile(file, "rw");
        raf.write("new file with longer data in the new file\n".getBytes());

        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).get(0).assertContentEquals("with longer data in the new file\n");
        runner.clearTransferState();
    }

    @Test
    public void testMultipleFiles() throws IOException, InterruptedException {
        runner.setProperty(TailFile.BASE_DIRECTORY, "target");
        runner.setProperty(TailFile.MODE, TailFile.MODE_MULTIFILE);
        final String fileRegex;
        if (File.separator.equals("/")) {
            fileRegex = "(testDir/)?log(ging)?.txt";
        } else {
            fileRegex = "(testDir" + Pattern.quote(File.separator) + ")?log(ging)?.txt";
        }
        runner.setProperty(TailFile.FILENAME, fileRegex);
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "${filename}.?");
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE);
        runner.setProperty(TailFile.RECURSIVE, "true");

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        // I manually add a third file to tail here
        // I'll remove it later in the test
        File thirdFile = new File("target/logging.txt");
        if(thirdFile.exists()) {
            thirdFile.delete();
        }
        assertTrue(thirdFile.createNewFile());
        RandomAccessFile thirdFileRaf = new RandomAccessFile(thirdFile, "rw");
        thirdFileRaf.write("hey\n".getBytes());

        otherRaf.write("hi\n".getBytes());
        raf.write("hello\n".getBytes());

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 3);
        Optional<MockFlowFile> thirdFileFF = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)
                .stream().filter(mockFlowFile -> mockFlowFile.isAttributeEqual("tailfile.original.path", thirdFile.getPath())).findFirst();
        assertTrue(thirdFileFF.isPresent());
        thirdFileFF.get().assertContentEquals("hey\n");
        Optional<MockFlowFile> otherFileFF = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)
                .stream().filter(mockFlowFile -> mockFlowFile.isAttributeEqual("tailfile.original.path", otherFile.getPath())).findFirst();
        assertTrue(otherFileFF.isPresent());
        otherFileFF.get().assertContentEquals("hi\n");
        Optional<MockFlowFile> fileFF = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)
                .stream().filter(mockFlowFile -> mockFlowFile.isAttributeEqual("tailfile.original.path", file.getPath())).findFirst();
        assertTrue(fileFF.isPresent());
        fileFF.get().assertContentEquals("hello\n");
        runner.clearTransferState();

        otherRaf.write("world!".getBytes());
        raf.write("world".getBytes());

        Thread.sleep(100L);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0); // should not pull in data because no \n

        raf.close();
        otherRaf.close();
        thirdFileRaf.close();
        thirdFile.delete();

        file.renameTo(new File("target/log.1"));
        otherFile.renameTo(new File("target/testDir/log.1"));

        raf = new RandomAccessFile(new File("target/log.txt"), "rw");
        raf.write("1\n".getBytes());

        otherRaf = new RandomAccessFile(new File("target/testDir/log.txt"), "rw");
        otherRaf.write("2\n".getBytes());

        // I also add a new file here
        File fourthFile = new File("target/testDir/logging.txt");
        if(fourthFile.exists()) {
            fourthFile.delete();
        }
        assertTrue(fourthFile.createNewFile());
        RandomAccessFile fourthFileRaf = new RandomAccessFile(fourthFile, "rw");
        fourthFileRaf.write("3\n".getBytes());
        fourthFileRaf.close();

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 5);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("3\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("world!")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("2\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("world")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("1\n")));
    }

    @Test
    public void testDetectNewFile() throws IOException, InterruptedException {
        runner.setProperty(TailFile.BASE_DIRECTORY, "target");
        runner.setProperty(TailFile.MODE, TailFile.MODE_MULTIFILE);
        runner.setProperty(TailFile.LOOKUP_FREQUENCY, "1 sec");
        runner.setProperty(TailFile.FILENAME, "log_[0-9]*\\.txt");
        runner.setProperty(TailFile.RECURSIVE, "false");
        runner.setProperty(TailFile.ROLLING_STRATEGY, TailFile.FIXED_NAME);

        initializeFile("target/log_1.txt", "firstLine\n");

        Runnable task = () -> {
            try {
                initializeFile("target/log_2.txt", "newFile\n");
            } catch (Exception e) {
                fail();
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.schedule(task, 2, TimeUnit.SECONDS);

        runner.setRunSchedule(2000);
        runner.run(3);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("firstLine\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("newFile\n")));

        runner.shutdown();
    }

    @Test
    public void testMultipleFilesWithBasedirAndFilenameEL() throws IOException, InterruptedException {
        runner.setVariable("vrBaseDirectory", "target");
        runner.setProperty(TailFile.BASE_DIRECTORY, "${vrBaseDirectory}");
        runner.setProperty(TailFile.MODE, TailFile.MODE_MULTIFILE);
        final String fileRegex;
        if (File.separator.equals("/")) {
            fileRegex = "(testDir/)?log(ging)?.txt";
        } else {
            fileRegex = "(testDir" + Pattern.quote(File.separator) + ")?log(ging)?.txt";
        }
        runner.setVariable("vrFilename", fileRegex);
        runner.setProperty(TailFile.FILENAME, "${vrFilename}");
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "${filename}.?");
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE);
        runner.setProperty(TailFile.RECURSIVE, "true");

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        otherRaf.write("hi\n".getBytes());
        raf.write("hello\n".getBytes());

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
    }

    /**
     * This test is used to check the case where we have multiple files in the same directory
     * and where it is not possible to specify a single rolling pattern for all files.
     */
    @Test
    public void testMultipleFilesInSameDirectory() throws IOException, InterruptedException {
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "${filename}.?");
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE);
        runner.setProperty(TailFile.BASE_DIRECTORY, "target");
        runner.setProperty(TailFile.FILENAME, "log(ging)?.txt");
        runner.setProperty(TailFile.MODE, TailFile.MODE_MULTIFILE);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);

        File myOtherFile = new File("target/logging.txt");
        if(myOtherFile.exists()) {
            myOtherFile.delete();
        }
        assertTrue(myOtherFile.createNewFile());

        RandomAccessFile myOtherRaf = new RandomAccessFile(myOtherFile, "rw");
        myOtherRaf.write("hey\n".getBytes());

        raf.write("hello\n".getBytes());

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        Optional<MockFlowFile> myOtherFileFF = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)
                .stream().filter(mockFlowFile -> mockFlowFile.isAttributeEqual("tailfile.original.path", myOtherFile.getPath())).findFirst();
        assertTrue(myOtherFileFF.isPresent());
        myOtherFileFF.get().assertContentEquals("hey\n");
        Optional<MockFlowFile> fileFF = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS)
                .stream().filter(mockFlowFile -> mockFlowFile.isAttributeEqual("tailfile.original.path", file.getPath())).findFirst();
        assertTrue(fileFF.isPresent());
        fileFF.get().assertContentEquals("hello\n");
        runner.clearTransferState();

        myOtherRaf.write("guys".getBytes());
        raf.write("world".getBytes());

        Thread.sleep(100L);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0); // should not pull in data because no \n

        raf.close();
        myOtherRaf.close();

        // roll over
        myOtherFile.renameTo(new File("target/logging.1"));
        file.renameTo(new File("target/log.1"));

        raf = new RandomAccessFile(new File("target/log.txt"), "rw");
        raf.write("1\n".getBytes());

        myOtherRaf = new RandomAccessFile(new File("target/logging.txt"), "rw");
        myOtherRaf.write("2\n".getBytes());
        myOtherRaf.close();

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 4);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("guys")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("2\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("world")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("1\n")));
    }

    @Test
    public void testMultipleFilesChangingNameStrategy() throws IOException, InterruptedException {
        runner.setProperty(TailFile.START_POSITION, TailFile.START_CURRENT_FILE);
        runner.setProperty(TailFile.MODE, TailFile.MODE_MULTIFILE);
        runner.setProperty(TailFile.ROLLING_STRATEGY, TailFile.CHANGING_NAME);
        runner.setProperty(TailFile.BASE_DIRECTORY, "target");
        runner.setProperty(TailFile.FILENAME, ".*app-.*.log");
        runner.setProperty(TailFile.LOOKUP_FREQUENCY, "2s");
        runner.setProperty(TailFile.MAXIMUM_AGE, "5s");

        File multiChangeFirstFile = new File("target/app-2016-09-07.log");
        if(multiChangeFirstFile.exists()) {
            multiChangeFirstFile.delete();
        }
        assertTrue(multiChangeFirstFile.createNewFile());

        RandomAccessFile multiChangeFirstRaf = new RandomAccessFile(multiChangeFirstFile, "rw");
        multiChangeFirstRaf.write("hey\n".getBytes());

        File multiChangeSndFile = new File("target/my-app-2016-09-07.log");
        if(multiChangeSndFile.exists()) {
            multiChangeSndFile.delete();
        }
        assertTrue(multiChangeSndFile.createNewFile());

        RandomAccessFile multiChangeSndRaf = new RandomAccessFile(multiChangeSndFile, "rw");
        multiChangeSndRaf.write("hello\n".getBytes());

        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hello\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hey\n")));
        runner.clearTransferState();

        multiChangeFirstRaf.write("hey2\n".getBytes());
        multiChangeSndRaf.write("hello2\n".getBytes());

        Thread.sleep(2000);
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 2);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hello2\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hey2\n")));
        runner.clearTransferState();

        multiChangeFirstRaf.write("hey3\n".getBytes());
        multiChangeSndRaf.write("hello3\n".getBytes());

        multiChangeFirstRaf.close();
        multiChangeSndRaf.close();

        multiChangeFirstFile = new File("target/app-2016-09-08.log");
        if(multiChangeFirstFile.exists()) {
            multiChangeFirstFile.delete();
        }
        assertTrue(multiChangeFirstFile.createNewFile());

        multiChangeFirstRaf = new RandomAccessFile(multiChangeFirstFile, "rw");
        multiChangeFirstRaf.write("hey\n".getBytes());

        multiChangeSndFile = new File("target/my-app-2016-09-08.log");
        if(multiChangeSndFile.exists()) {
            multiChangeSndFile.delete();
        }
        assertTrue(multiChangeSndFile.createNewFile());

        multiChangeSndRaf = new RandomAccessFile(multiChangeSndFile, "rw");
        multiChangeSndRaf.write("hello\n".getBytes());

        Thread.sleep(2000);
        runner.run(1);
        multiChangeFirstRaf.close();
        multiChangeSndRaf.close();

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 4);
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hello3\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hello\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hey3\n")));
        assertTrue(runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).stream().anyMatch(mockFlowFile -> mockFlowFile.isContentEqual("hey\n")));
        runner.clearTransferState();
    }

    private boolean isWindowsEnvironment() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    @Test
    public void testMigrateFrom100To110() throws IOException {
        assumeFalse(isWindowsEnvironment());
        runner.setProperty(TailFile.FILENAME, "target/existing-log.txt");

        final MockStateManager stateManager = runner.getStateManager();

        // Before NiFi 1.1.0, TailFile only handles single file
        // and state key doesn't have index in it.
        final Map<String, String> state = new HashMap<>();
        state.put("filename", "target/existing-log.txt");
        // Simulate that it has been tailed up to the 2nd line.
        state.put("checksum", "2279929157");
        state.put("position", "14");
        state.put("timestamp", "1480639134000");
        stateManager.setState(state, Scope.LOCAL);

        runner.run();

        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS).iterator().next();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(bos))) {
            writer.write("Line 3");
            writer.newLine();
        }

        flowFile.assertContentEquals(bos.toByteArray());

        // The old states should be replaced with new ones.
        final StateMap updatedState = stateManager.getState(Scope.LOCAL);
        assertNull(updatedState.get("filename"));
        assertNull(updatedState.get("checksum"));
        assertNull(updatedState.get("position"));
        assertNull(updatedState.get("timestamp"));
        assertEquals("target/existing-log.txt", updatedState.get("file.0.filename"));
        assertEquals("3380848603", updatedState.get("file.0.checksum"));
        assertEquals("21", updatedState.get("file.0.position"));
        assertNotNull(updatedState.get("file.0.timestamp"));

        // When it runs again, the state is already migrated, so it shouldn't emit any flow files.
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(TailFile.REL_SUCCESS, 0);
    }


    @Test
    public void testMigrateFrom100To110FileNotFound() throws IOException {
        assumeFalse(isWindowsEnvironment());

        runner.setProperty(TailFile.FILENAME, "target/not-existing-log.txt");

        final MockStateManager stateManager = runner.getStateManager();

        // Before NiFi 1.1.0, TailFile only handles single file
        // and state key doesn't have index in it.
        final Map<String, String> state = new HashMap<>();
        state.put("filename", "target/not-existing-log.txt");
        // Simulate that it has been tailed up to the 2nd line.
        state.put("checksum", "2279929157");
        state.put("position", "14");
        state.put("timestamp", "1480639134000");
        stateManager.setState(state, Scope.LOCAL);

        runner.run();

        runner.assertTransferCount(TailFile.REL_SUCCESS, 0);
    }

    private void cleanFiles(String directory) {
        final File targetDir = new File(directory);
        if(targetDir.exists()) {
            final File[] files = targetDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(final File dir, final String name) {
                    return name.startsWith("log") || name.endsWith("log");
                }
            });

            for (final File file : files) {
                file.delete();
            }
        }
    }

    private void clean() {
        cleanFiles("target");
        cleanFiles("target/testDir");
    }

    private RandomAccessFile initializeFile(String path, String data) throws IOException {
        File file = new File(path);
        if(file.exists()) {
            file.delete();
        }
        assertTrue(file.createNewFile());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.write(data.getBytes());
        randomAccessFile.close();
        return randomAccessFile;
    }

}
