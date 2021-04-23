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

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractTestTailFileScenario {
    public static final String TEST_DIRECTORY = "testTailFileScenario";
    public static final String TARGET_FILE_PATH = "target/" + TEST_DIRECTORY + "/in.txt";
    public static final String NUL_SUBSTITUTE = "X";
    public static final Long POST_ROLLOVER_WAIT_PERSIOD_SECONDS = 100L;

    protected File file;
    protected RandomAccessFile randomAccessFile;

    private TailFile processor;
    protected TestRunner runner;

    private AtomicBoolean stopAfterEachTrigger;

    protected AtomicLong wordIndex;
    protected AtomicLong rolloverIndex;
    protected AtomicLong timeAdjustment;
    protected AtomicBoolean rolloverSwitchPending;

    protected LinkedList<Long> nulPositions;

    protected List<String> expected;
    private Random random;

    @Before
    public void setUp() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "TRACE");

        clean();

        File directory = new File("target/" + TEST_DIRECTORY);
        if (!directory.exists()) {
            assertTrue(directory.mkdirs());
        }

        createTargetFile();
        randomAccessFile = new RandomAccessFile(file, "rw");

        processor = new TailFile() {
            @Override
            public long getCurrentTimeMs() {
                return super.getCurrentTimeMs() + timeAdjustment.get();
            }
        };
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TailFile.FILENAME, TARGET_FILE_PATH);
        runner.setProperty(TailFile.ROLLING_FILENAME_PATTERN, "in.txt*");
        runner.setProperty(TailFile.REREAD_ON_NUL, "true");
        runner.setProperty(TailFile.POST_ROLLOVER_TAIL_PERIOD, POST_ROLLOVER_WAIT_PERSIOD_SECONDS + " sec");
        runner.assertValid();

        runner.run(1, false, true);

        stopAfterEachTrigger = new AtomicBoolean(false);

        nulPositions = new LinkedList<>();
        wordIndex = new AtomicLong(1);
        rolloverIndex = new AtomicLong(1);
        timeAdjustment = new AtomicLong(0);
        rolloverSwitchPending = new AtomicBoolean(false);

        expected = new ArrayList<>();

        random = new Random();
    }

    @After
    public void tearDown() throws IOException {
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }

        processor.cleanup();
    }

    public void testScenario(List<Action> actions) throws Exception {
        testScenario(actions, false);

        tearDown();
        setUp();

        testScenario(actions, true);
    }

    public void testScenario(List<Action> actions, boolean stopAfterEachTrigger) throws Exception {
        if (actions.contains(Action.ROLLOVER)) {
            Assume.assumeTrue("Test wants to rename an open file which is not allowed on Windows", !SystemUtils.IS_OS_WINDOWS);
        }

        // GIVEN
        this.stopAfterEachTrigger.set(stopAfterEachTrigger);

        // WHEN
        for (Action action : actions) {
            action.run(this);
        }
        overwriteRemainingNuls();
        Action.WRITE_NEW_LINE.run(this);
        Action.TRIGGER.run(this);
        Action.EXPIRE_ROLLOVER_WAIT_PERIOD.run(this);
        Action.TRIGGER.run(this);
        Action.TRIGGER.run(this);

        // THEN
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(TailFile.REL_SUCCESS);
        List<String> actual = flowFiles.stream()
            .map(MockFlowFile::toByteArray)
            .map(String::new)
            .collect(Collectors.toList());

        assertEquals(
            stopAfterEachTrigger + " " + actions.toString(),
            expected.stream().collect(Collectors.joining()),
            actual.stream().collect(Collectors.joining())
        );
    }

    private void clean() {
        cleanFiles("target/" + TEST_DIRECTORY);
    }

    private void cleanFiles(String directory) {
        final File targetDir = new File(directory);

        if (targetDir.exists()) {
            for (final File file : targetDir.listFiles()) {
                file.delete();
            }
        }
    }

    private void createTargetFile() throws IOException {
        file = new File(TARGET_FILE_PATH);
        file.delete();
        assertTrue(file.createNewFile());
    }

    private void overwriteRemainingNuls() throws Exception {
        while (!nulPositions.isEmpty()) {
            Action.OVERWRITE_NUL.run(this);
        }
    }

    private void writeWord() throws IOException {
        String word = "-word_" + wordIndex.getAndIncrement() + "-";

        randomAccessFile.write(word.getBytes());

        expected.add(word);
    }

    private void writeNewLine() throws IOException {
        randomAccessFile.write("\n".getBytes());

        expected.add("\n");
    }

    private void writeNul() throws IOException {
        nulPositions.add(randomAccessFile.getFilePointer());

        randomAccessFile.write("\0".getBytes());

        expected.add(NUL_SUBSTITUTE);
    }

    private void overwriteNul() throws IOException {
        if (!nulPositions.isEmpty()) {
            Long nulPosition = nulPositions.remove(random.nextInt(nulPositions.size()));

            long currentPosition = randomAccessFile.getFilePointer();
            randomAccessFile.seek(nulPosition);
            randomAccessFile.write(NUL_SUBSTITUTE.getBytes());
            randomAccessFile.seek(currentPosition);
        }
    }

    private void trigger() {
        runner.run(1, stopAfterEachTrigger.get(), false);
    }

    private void rollover() throws IOException {
        File rolledOverFile = new File(file.getParentFile(), file.getName() + "." + rolloverIndex.getAndIncrement());
        file.renameTo(rolledOverFile);

        createTargetFile();

        rolloverSwitchPending.set(true);
    }

    private void switchFile() throws Exception {
        if (rolloverSwitchPending.get()) {
            overwriteRemainingNuls();

            randomAccessFile.close();
            randomAccessFile = new RandomAccessFile(file, "rw");

            rolloverSwitchPending.set(false);
        }
    }

    private void expireRolloverWaitPeriod() throws Exception {
        long waitPeriod = POST_ROLLOVER_WAIT_PERSIOD_SECONDS * 1000 + 100;
        timeAdjustment.set(timeAdjustment.get() + waitPeriod);
    }

    protected enum Action {
        WRITE_WORD(AbstractTestTailFileScenario::writeWord),
        WRITE_NEW_LINE(AbstractTestTailFileScenario::writeNewLine),
        WRITE_NUL(AbstractTestTailFileScenario::writeNul),
        OVERWRITE_NUL(AbstractTestTailFileScenario::overwriteNul),
        TRIGGER(AbstractTestTailFileScenario::trigger),
        ROLLOVER(AbstractTestTailFileScenario::rollover),
        SWITCH_FILE(AbstractTestTailFileScenario::switchFile),
        EXPIRE_ROLLOVER_WAIT_PERIOD(AbstractTestTailFileScenario::expireRolloverWaitPeriod);

        private final ActionRunner actionRunner;

        Action(ActionRunner actionRunner) {
            this.actionRunner = actionRunner;
        }

        void run(AbstractTestTailFileScenario currentTest) throws Exception {
            actionRunner.runAction(currentTest);
        }
    }

    private interface ActionRunner {
        void runAction(AbstractTestTailFileScenario currentTest) throws Exception;
    }
}
