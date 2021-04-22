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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestTailFileSimpleScenarios extends AbstractTestTailFileScenario {
    @Test
    public void testSimpleScenario() throws Exception {
        // GIVEN
        List<Action> actions = Arrays.asList(
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.TRIGGER,
            Action.ROLLOVER,
            Action.TRIGGER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.WRITE_WORD, Action.WRITE_NUL,
            Action.OVERWRITE_NUL, Action.WRITE_NEW_LINE,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.SWITCH_FILE,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE
        );

        // WHEN
        // THEN
        testScenario(actions);
    }

    @Test
    public void testSimpleScenario2() throws Exception {
        // GIVEN
        List<Action> actions = Arrays.asList(
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.WRITE_WORD,
            Action.WRITE_NUL,
            Action.TRIGGER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.OVERWRITE_NUL
        );

        // WHEN
        // THEN
        testScenario(actions);
    }

    @Test
    public void testSimpleScenario3() throws Exception {
        // GIVEN
        List<Action> actions = Arrays.asList(
            Action.WRITE_WORD,
            Action.WRITE_NUL,
            Action.TRIGGER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.OVERWRITE_NUL
        );

        // WHEN
        // THEN
        testScenario(actions);
    }

    @Test
    public void testSimpleScenario4() throws Exception {
        // GIVEN
        List<Action> actions = Arrays.asList(
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.ROLLOVER,
            Action.TRIGGER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.SWITCH_FILE,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE
        );

        // WHEN
        // THEN
        testScenario(actions);
    }

    @Test
    public void testSimpleScenario5() throws Exception {
        // GIVEN
        List<Action> actions = Arrays.asList(
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.TRIGGER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE,
            Action.WRITE_NUL,
            Action.TRIGGER,
            Action.OVERWRITE_NUL,
            Action.ROLLOVER,
            Action.WRITE_WORD, Action.WRITE_NEW_LINE
        );

        // WHEN
        // THEN
        testScenario(actions);
    }
}
