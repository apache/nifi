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
package org.apache.nifi.processors.aws.kinesis.stream.pause;

import org.apache.nifi.processor.ProcessContext;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.concurrent.CountDownLatch;

import static org.apache.nifi.processors.aws.v2.AbstractAwsProcessor.REL_SUCCESS;

public class PauseImpl implements PauseConsumer {

    private final PauseCondition pauseCondition;
    private final PauseInspector pauseInspector;
    private CountDownLatch isPaused = new CountDownLatch(0);
    private boolean skipPause = false;

    @VisibleForTesting
    public static PauseImpl createNonHalting() {
        return new PauseImpl(() -> false, new NoopPauseInspector());
    }

    @VisibleForTesting
    static PauseImpl create(final PauseInspector pauseInspector) {
        return new PauseImpl(new PauseConditionRelationUnavailability(REL_SUCCESS), pauseInspector);
    }

    public static PauseImpl create() {
        return new PauseImpl(new PauseConditionRelationUnavailability(REL_SUCCESS), new NoopPauseInspector());
    }

    private PauseImpl(final PauseCondition pauseCondition, final PauseInspector pauseInspector) {
        this.pauseCondition = pauseCondition;
        this.pauseInspector = pauseInspector;
    }

    public synchronized void init(final ProcessContext context) {
        skipPause = false;
        onTrigger(context);
    }

    public void consumePause() throws InterruptedException {
        try {
            pauseInspector.onPauseAwaited();
            awaitPauseEnd();
        } finally {
            pauseInspector.onPauseFinished();
        }
    }

    private void awaitPauseEnd() throws InterruptedException {
        isPaused.await();
    }

    public synchronized void onTrigger(final ProcessContext context) {
        pauseCondition.onTrigger(context);
        boolean shouldPause = pauseCondition.shouldPause();
        if (!shouldPause) {
            isPaused.countDown();
        } else if (!skipPause) {
            isPaused = isPaused.getCount() > 0
                    ? isPaused
                    : new CountDownLatch(1);
        }
    }

    public synchronized void unpause() {
        skipPause = true;
        isPaused.countDown();
    }

    private static class NoopPauseInspector implements PauseInspector { }
}
