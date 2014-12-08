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
package org.apache.nifi.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link UpdateMonitor} that combines multiple <code>UpdateMonitor</code>s
 * such that it will indicate a change in a file only if ALL sub-monitors
 * indicate a change. The sub-monitors will be applied in the order given and if
 * any indicates that the state has not changed, the subsequent sub-monitors may
 * not be given a chance to run
 */
public class CompoundUpdateMonitor implements UpdateMonitor {

    private final List<UpdateMonitor> monitors;

    public CompoundUpdateMonitor(final UpdateMonitor first, final UpdateMonitor... others) {
        monitors = new ArrayList<>();
        monitors.add(first);
        for (final UpdateMonitor monitor : others) {
            monitors.add(monitor);
        }
    }

    @Override
    public Object getCurrentState(final Path path) throws IOException {
        return new DeferredMonitorAction(monitors, path);
    }

    private static class DeferredMonitorAction {

        private static final Object NON_COMPUTED_VALUE = new Object();

        private final List<UpdateMonitor> monitors;
        private final Path path;

        private final Object[] preCalculated;

        public DeferredMonitorAction(final List<UpdateMonitor> monitors, final Path path) {
            this.monitors = monitors;
            this.path = path;
            preCalculated = new Object[monitors.size()];

            for (int i = 0; i < preCalculated.length; i++) {
                preCalculated[i] = NON_COMPUTED_VALUE;
            }
        }

        private Object getCalculatedValue(final int i) throws IOException {
            if (preCalculated[i] == NON_COMPUTED_VALUE) {
                preCalculated[i] = monitors.get(i).getCurrentState(path);
            }

            return preCalculated[i];
        }

        @Override
        public boolean equals(final Object obj) {
            // must return true unless ALL DeferredMonitorAction's indicate that they are different
            if (obj == null) {
                return false;
            }

            if (!(obj instanceof DeferredMonitorAction)) {
                return false;
            }

            final DeferredMonitorAction other = (DeferredMonitorAction) obj;
            try {
                // Go through each UpdateMonitor's value and check if the value has changed.
                for (int i = 0; i < preCalculated.length; i++) {
                    final Object mine = getCalculatedValue(i);
                    final Object theirs = other.getCalculatedValue(i);

                    if (mine == theirs) {
                        // same
                        return true;
                    }

                    if (mine == null && theirs == null) {
                        // same
                        return true;
                    }

                    if (mine.equals(theirs)) {
                        return true;
                    }
                }
            } catch (final IOException e) {
                return false;
            }

            // No DeferredMonitorAction was the same as last time. Therefore, it's not equal
            return false;
        }
    }
}
