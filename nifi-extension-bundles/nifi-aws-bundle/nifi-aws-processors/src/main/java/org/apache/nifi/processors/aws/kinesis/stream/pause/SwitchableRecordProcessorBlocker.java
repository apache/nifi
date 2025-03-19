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

import java.util.concurrent.CountDownLatch;

public class SwitchableRecordProcessorBlocker implements RecordProcessorBlocker {

    private final SwitchableRecordProcessorBlockerInspector switchableRecordProcessorBlockerInspector;
    private CountDownLatch isPaused = new CountDownLatch(0);

    public static SwitchableRecordProcessorBlocker createNonBlocking() {
        return new SwitchableRecordProcessorBlocker(new NoopSwitchableRecordProcessorBlockerInspector()) {
            @Override
            public synchronized void block() {
                // don't block
            }
        };
    }

    static SwitchableRecordProcessorBlocker create(final SwitchableRecordProcessorBlockerInspector switchableRecordProcessorBlockerInspector) {
        return new SwitchableRecordProcessorBlocker(switchableRecordProcessorBlockerInspector);
    }

    public static SwitchableRecordProcessorBlocker create() {
        return new SwitchableRecordProcessorBlocker(new NoopSwitchableRecordProcessorBlockerInspector());
    }

    private SwitchableRecordProcessorBlocker(final SwitchableRecordProcessorBlockerInspector switchableRecordProcessorBlockerInspector) {
        this.switchableRecordProcessorBlockerInspector = switchableRecordProcessorBlockerInspector;
    }

    public void await() throws InterruptedException {
        try {
            switchableRecordProcessorBlockerInspector.onPauseAwaited();
            isPaused.await();
        } finally {
            switchableRecordProcessorBlockerInspector.onPauseFinished();
        }
    }

    public synchronized void block() {
        isPaused = isPaused.getCount() > 0
                ? isPaused
                : new CountDownLatch(1);
    }

    public synchronized void unblock() {
        isPaused.countDown();
    }

    private static class NoopSwitchableRecordProcessorBlockerInspector implements SwitchableRecordProcessorBlockerInspector { }
}
