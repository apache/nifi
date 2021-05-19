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

package org.apache.nifi.stateless.flow;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface DataflowTrigger {

    /**
     * Cancels the triggering of the dataflow. If the dataflow has not yet completed, any actions left to perform
     * will not be completed. The session will be rolled back instead of committed.
     */
    void cancel();

    /**
     * Returns the results of triggering the dataflow immediately, if they are available, else returns an empty Optional
     * @return the results of triggering the dataflow immediately, if they are available, else returns an empty Optional
     */
    Optional<TriggerResult> getResultNow();

    /**
     * Waits up to the specified amount of time for the result to be come available. If, after that time, the result is not
     * available, returns an empty Optional. Otherwise, returns the results as soon as they become available.
     *
     * @param maxWaitTime the maximum amount of time to wait
     * @param timeUnit the time unit that the max wait time is associated with
     * @return the results of triggering the dataflow, or an empty Optional if the results are not available within the given amount of time
     * @throws InterruptedException if interrupted while waiting for the results
     */
    Optional<TriggerResult> getResult(long maxWaitTime, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Returns the results of triggering the dataflow, waiting as long as necessary for the results to become available.
     * @return the results of triggering the dataflow
     * @throws InterruptedException if interrupted while waiting for the results
     */
    TriggerResult getResult() throws InterruptedException;
}
