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
package org.apache.nifi.util;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistryUtils;
import org.junit.Assert;
import org.junit.Test;

public class CurrentTestStandardProcessorTestRunner {

    /**
     * This test will verify that all iterations of the run are finished before unscheduled is called
     */
    @Test
    public void testOnScheduledCalledAfterRunFinished() {
        SlowRunProcessor processor = new SlowRunProcessor();
        StandardProcessorTestRunner runner = new StandardProcessorTestRunner(processor, VariableRegistryUtils.createSystemVariableRegistry());
        final int iterations = 5;
        runner.run(iterations);
        // if the counter is not equal to iterations, the the processor must have been unscheduled
        // before all the run calls were made, that would be bad.
        Assert.assertEquals(iterations, processor.getCounter());
    }

    /**
     * This processor simulates a "slow" processor that checks whether it is scheduled before doing something
     *
     *
     */
    private static class SlowRunProcessor extends AbstractProcessor {

        private int counter = 0;

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            try {
                // be slow
                Thread.sleep(50);
                // make sure we are still scheduled
                if (isScheduled()) {
                    // increment counter
                    ++counter;
                }
            } catch (InterruptedException e) {
            }

        }

        public int getCounter() {
            return counter;
        }
    }
}
