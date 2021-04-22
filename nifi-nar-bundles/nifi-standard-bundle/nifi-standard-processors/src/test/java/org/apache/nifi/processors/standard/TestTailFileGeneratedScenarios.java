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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Ignore("Stress test - longrunning. For manual testing.")
@RunWith(Parameterized.class)
public class TestTailFileGeneratedScenarios extends AbstractTestTailFileScenario {
    private final List<Action> actions;

    public TestTailFileGeneratedScenarios(List<Action> actions) {
        this.actions = actions;
    }

    @Parameterized.Parameters
    public static Collection parameters() {
        Collection<Object[]> parameters = new ArrayList();

        // Uncomment the portion for which to run the scenarios.
        //  They cannot be added to a single large batch because it opens too many files.
//        List<Action> baseActions = Arrays.asList(
//            Action.WRITE_WORD,
//            Action.WRITE_NUL,
//            Action.WRITE_NEW_LINE,
//            Action.WRITE_NUL,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_WORD,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_NUL,
//            Action.WRITE_NEW_LINE,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_NUL,
//            Action.OVERWRITE_NUL
//        );
//        addAction(parameters, Action.TRIGGER, baseActions, 0, 0);
//        new ArrayList<>(parameters).forEach(anActionList -> addAction(parameters, Action.ROLLOVER, (List<Action>)anActionList[0], 0, 0));
//        new ArrayList<>(parameters).forEach(anActionList -> addAction(parameters, Action.SWITCH_FILE, (List<Action>)anActionList[0], 0, 0));

//        List<Action> baseActions = Arrays.asList(
//            Action.WRITE_WORD,
//            Action.WRITE_NEW_LINE,
//            Action.WRITE_NUL,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_WORD,
//            Action.WRITE_NUL,
//            Action.WRITE_NEW_LINE,
//            Action.OVERWRITE_NUL,
//            Action.ROLLOVER,
//            Action.WRITE_WORD,
//            Action.WRITE_NEW_LINE,
//            Action.WRITE_NUL,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_WORD,
//            Action.WRITE_NUL,
//            Action.WRITE_NEW_LINE,
//            Action.OVERWRITE_NUL,
//            Action.SWITCH_FILE,
//            Action.WRITE_WORD,
//            Action.WRITE_NEW_LINE,
//            Action.WRITE_NUL,
//            Action.OVERWRITE_NUL,
//            Action.WRITE_WORD,
//            Action.WRITE_NUL,
//            Action.WRITE_NEW_LINE,
//            Action.OVERWRITE_NUL
//        );
//        addAction(parameters, Action.TRIGGER, baseActions, 0, 1);

        List<Action> baseActions = Arrays.asList(
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

        return parameters;
    }

    private static void addAction(Collection<Object[]> parameters, Action action, List<Action> baseActions, int currentDepth, int recursiveDepth) {
        for (int triggerIndex = 0; triggerIndex < baseActions.size(); triggerIndex++) {
            List<Action> actions = new LinkedList<>(baseActions);
            actions.add(triggerIndex, action);

            parameters.add(new Object[]{ actions});

            if (currentDepth < recursiveDepth) {
                addAction(parameters, action, actions, currentDepth+1, recursiveDepth);
            }
        }
    }

    @Test
    public void testScenario() throws Exception {
        testScenario(actions);
    }
}
