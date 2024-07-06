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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

@EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true", disabledReason = "Stress test - longrunning. For manual testing.")
public class TestTailFileGeneratedScenarios extends AbstractTestTailFileScenario {
    private static List<Arguments> parameters = new ArrayList<>();

    @BeforeAll
    public static final void createParameters() {

        final List<Action> baseActions = Arrays.asList(
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_NUL, Action.WRITE_NUL,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.OVERWRITE_NUL, Action.OVERWRITE_NUL,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.ROLLOVER,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_NUL, Action.WRITE_NUL,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.OVERWRITE_NUL, Action.OVERWRITE_NUL,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.SWITCH_FILE,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_NUL, Action.WRITE_NUL,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.OVERWRITE_NUL, Action.OVERWRITE_NUL,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE,
                Action.WRITE_WORD, Action.WRITE_WORD,
                Action.WRITE_NEW_LINE, Action.WRITE_NEW_LINE
        );
        addAction(parameters, Action.TRIGGER, baseActions, 0, 1);
    }

    private static Stream<Arguments> provideActionsForTestScenario() {
        return parameters.stream();
    }

    private static void addAction(List<Arguments> parameters, Action action, List<Action> baseActions, int currentDepth, int recursiveDepth) {
        for (int triggerIndex = 0; triggerIndex < baseActions.size(); triggerIndex++) {
            List<Action> actions = new LinkedList<>(baseActions);
            actions.add(triggerIndex, action);

            parameters.add(Arguments.of(actions));

            if (currentDepth < recursiveDepth) {
                addAction(parameters, action, actions, currentDepth + 1, recursiveDepth);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideActionsForTestScenario")
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Test wants to rename an open file which is not allowed on Windows")
    public void testParameterizedScenario(List<Action> actions) throws Exception {
        testScenario(actions);
    }
}
