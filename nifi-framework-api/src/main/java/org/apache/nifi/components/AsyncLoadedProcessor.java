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

package org.apache.nifi.components;

import org.apache.nifi.processor.Processor;

public interface AsyncLoadedProcessor extends Processor {
    default boolean isLoaded() {
        return getState() == LoadState.FINISHED_LOADING;
    }

    LoadState getState();

    /**
     * Cancels the async loading process if it is still in progress.
     * This is typically called when the component is being removed or the NAR
     * containing the processor is being deleted while the processor is still initializing.
     *
     * <p>After cancellation, the processor should transition to the {@link LoadState#CANCELLED} state.</p>
     *
     * <p>The default implementation does nothing, allowing implementations that don't support
     * cancellation to work without changes.</p>
     */
    default void cancelLoading() {
        // Default implementation does nothing
    }

    enum LoadState {
        INITIALIZING_ENVIRONMENT,

        DOWNLOADING_DEPENDENCIES,

        LOADING_PROCESSOR_CODE,

        DEPENDENCY_DOWNLOAD_FAILED,

        LOADING_PROCESSOR_CODE_FAILED,

        FINISHED_LOADING,

        /**
         * Indicates that the processor initialization was cancelled before it could complete.
         * This typically happens when the NAR containing the processor is deleted while
         * the processor is still initializing.
         */
        CANCELLED
    }
}
