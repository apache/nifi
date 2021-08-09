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

package org.apache.nifi.stateless.engine;

/**
 * The ExecutionProgress functions as a bridge between the caller of the dataflow trigger
 * and the dataflow engine. It is used to allow the caller to cancel the dataflow, wait for its completion, and to convey
 * to the engine that the user has canceled the dataflow, or determine whether or not all relevant data has been processed
 */
public interface ExecutionProgress {

    /**
     * @return <code>true</code> if the execution has been canceled, <code>false</code> otherwise
     */
    boolean isCanceled();

    /**
     * @return <code>true</code> if there is data queued up to be processed, <code>false</code> if all data has been removed from the flow
     * or queued at Terminal Output Ports
     */
    boolean isDataQueued();

    /**
     * Returns the Completion Action that should be taken when the dataflow has completed, blocking as long as necessary for the determination to be made
     * @return the CompletionAction that should be taken
     */
    CompletionAction awaitCompletionAction() throws InterruptedException;

    /**
     * Notifies the ExecutionProgress that processing has been canceled
     */
    void notifyExecutionCanceled();

    /**
     * Notifies the ExecutionProgress that processing has failed
     */
    void notifyExecutionFailed(Throwable cause);


    enum CompletionAction {
        COMPLETE,

        CANCEL;
    }
}
