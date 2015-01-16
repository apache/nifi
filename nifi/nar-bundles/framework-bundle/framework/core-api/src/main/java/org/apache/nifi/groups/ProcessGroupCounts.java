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
package org.apache.nifi.groups;

public class ProcessGroupCounts {

    private final int inputPortCount, outputPortCount, runningCount, stoppedCount, invalidCount, disabledCount, activeRemotePortCount, inactiveRemotePortCount;

    public ProcessGroupCounts(final int inputPortCount, final int outputPortCount, final int runningCount,
            final int stoppedCount, final int invalidCount, final int disabledCount, final int activeRemotePortCount, final int inactiveRemotePortCount) {
        this.inputPortCount = inputPortCount;
        this.outputPortCount = outputPortCount;
        this.runningCount = runningCount;
        this.stoppedCount = stoppedCount;
        this.invalidCount = invalidCount;
        this.disabledCount = disabledCount;
        this.activeRemotePortCount = activeRemotePortCount;
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    public int getInputPortCount() {
        return inputPortCount;
    }

    public int getOutputPortCount() {
        return outputPortCount;
    }

    public int getRunningCount() {
        return runningCount;
    }

    public int getStoppedCount() {
        return stoppedCount;
    }

    public int getInvalidCount() {
        return invalidCount;
    }

    public int getDisabledCount() {
        return disabledCount;
    }

    public int getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public int getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }
}
