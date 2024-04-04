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

package org.apache.nifi.web.api.concurrent;

import org.apache.nifi.authorization.user.NiFiUser;

import java.util.Date;
import java.util.List;

public interface AsynchronousWebRequest<R, T> {

    /**
     * @return the ID of the component that the request is for
     */
    String getComponentId();

    /**
     * @return whether or not this request has completed
     */
    boolean isComplete();

    /**
     * @return the Date at which the status of this request was last updated
     */
    Date getLastUpdated();

    List<UpdateStep> getUpdateSteps();

    UpdateStep getCurrentStep();

    void markStepComplete();

    void markStepComplete(T results);

    String getState();

    R getRequest();

    /**
     * @return the current percent complete, between 0 and 100 (inclusive)
     */
    int getPercentComplete();

    /**
     * @return the user who submitted the request
     */
    NiFiUser getUser();

    /**
     * Updates the request to indicate the reason that the request failed
     *
     * @param explanation the reason that the request failed
     */
    void fail(String explanation);

    /**
     * Indicates the reason that the request failed, or <code>null</code> if the request has not failed
     *
     * @return the reason that the request failed, or <code>null</code> if the request has not failed
     */
    String getFailureReason();

    /**
     * Returns the results of the request, if it completed successfully, or <code>null</code> if the request either has no completed or failed
     *
     * @return the results of the request, if it completed successfully, or <code>null</code> if the request either has no completed or failed
     */
    T getResults();

    /**
     * Cancels the request so that no more steps can be completed
     */
    void cancel();

    /**
     * @return <code>true</code> if the request has been canceled, <code>false</code> otherwise
     */
    boolean isCancelled();

    /**
     * Sets the cancel callback to the given runnable, so that if {@link #cancel()} is called, the given {@link Runnable} will be triggered.
     * If <code>null</code> is passed, no operation will be triggered when the task is cancelled.
     *
     * @param runnable the callback
     */
    void setCancelCallback(Runnable runnable);
}
