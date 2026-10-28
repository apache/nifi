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
package org.apache.nifi.controller.scheduling;

/**
 * Receives scheduling observations from a Process Session.
 */
public interface SessionSchedulingObserver {

    SessionSchedulingObserver NO_OP = new SessionSchedulingObserver() {
        @Override
        public void onActivity() {
        }

        @Override
        public void onCommit(final CommittedSchedulingWork work) {
        }

        @Override
        public void onCommitFailure() {
        }
    };

    /** Records Process Session activity that prevents classifying an invocation as empty. */
    void onActivity();

    /** Records work after a complete successful commit. */
    void onCommit(CommittedSchedulingWork work);

    /** Records a failed Process Session commit. */
    void onCommitFailure();
}
