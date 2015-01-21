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
package org.apache.nifi.util.file.monitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Allows the user to configure a {@link java.nio.file.Path Path} to watch for
 * modifications and periodically poll to check if the file has been modified
 */
public class SynchronousFileWatcher {

    private final Path path;
    private final long checkUpdateMillis;
    private final UpdateMonitor monitor;
    private final AtomicReference<StateWrapper> lastState;
    private final Lock resourceLock = new ReentrantLock();

    public SynchronousFileWatcher(final Path path, final UpdateMonitor monitor) {
        this(path, monitor, 0L);
    }

    public SynchronousFileWatcher(final Path path, final UpdateMonitor monitor, final long checkMillis) {
        if (checkMillis < 0) {
            throw new IllegalArgumentException();
        }

        this.path = path;
        checkUpdateMillis = checkMillis;
        this.monitor = monitor;

        Object currentState;
        try {
            currentState = monitor.getCurrentState(path);
        } catch (final IOException e) {
            currentState = null;
        }

        this.lastState = new AtomicReference<>(new StateWrapper(currentState));
    }

    /**
     * Checks if the file has been updated according to the configured
     * {@link UpdateMonitor} and resets the state
     *
     * @return
     * @throws IOException
     */
    public boolean checkAndReset() throws IOException {
        if (checkUpdateMillis <= 0) { // if checkUpdateMillis <= 0, always check
            return checkForUpdate();
        } else {
            final StateWrapper stateWrapper = lastState.get();
            if (stateWrapper.getTimestamp() < System.currentTimeMillis() - checkUpdateMillis) {
                return checkForUpdate();
            }
            return false;
        }
    }

    private boolean checkForUpdate() throws IOException {
        if (resourceLock.tryLock()) {
            try {
                final StateWrapper wrapper = lastState.get();
                final Object newState = monitor.getCurrentState(path);
                if (newState == null && wrapper.getState() == null) {
                    return false;
                }
                if (newState == null || wrapper.getState() == null) {
                    lastState.set(new StateWrapper(newState));
                    return true;
                }

                final boolean unmodified = newState.equals(wrapper.getState());
                if (!unmodified) {
                    lastState.set(new StateWrapper(newState));
                }
                return !unmodified;
            } finally {
                resourceLock.unlock();
            }
        } else {
            return false;
        }
    }

    private static class StateWrapper {

        private final Object state;
        private final long timestamp;

        public StateWrapper(final Object state) {
            this.state = state;
            this.timestamp = System.currentTimeMillis();
        }

        public Object getState() {
            return state;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
