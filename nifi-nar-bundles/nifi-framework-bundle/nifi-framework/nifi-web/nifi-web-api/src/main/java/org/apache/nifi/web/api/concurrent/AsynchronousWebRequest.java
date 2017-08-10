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

import java.util.Date;

import org.apache.nifi.authorization.user.NiFiUser;

public interface AsynchronousWebRequest<T> {

    /**
     * @return the ID of the process group that the request is for
     */
    String getProcessGroupId();

    /**
     * @return whether or not this request has completed
     */
    boolean isComplete();

    /**
     * @return the Date at which the status of this request was last updated
     */
    Date getLastUpdated();

    /**
     * Updates the Date at which the status of this request was last updated
     *
     * @param date the date at which the status of this request was last updated
     */
    void setLastUpdated(Date date);

    /**
     * @return the user who submitted the request
     */
    NiFiUser getUser();

    /**
     * Indicates that this request has completed, successfully or otherwise
     *
     * @param results the results of the request
     */
    void markComplete(T results);

    /**
     * Updates the request to indicate the reason that the request failed
     *
     * @param explanation the reason that the request failed
     */
    void setFailureReason(String explanation);

    /**
     * Indicates the reason that the request failed, or <code>null</code> if the request has not failed
     *
     * @param explanation the reason that the request failed, or <code>null</code> if the request has not failed
     */
    String getFailureReason();

    /**
     * Returns the results of the request, if it completed successfully, or <code>null</code> if the request either has no completed or failed
     *
     * @return the results of the request, if it completed successfully, or <code>null</code> if the request either has no completed or failed
     */
    T getResults();
}
