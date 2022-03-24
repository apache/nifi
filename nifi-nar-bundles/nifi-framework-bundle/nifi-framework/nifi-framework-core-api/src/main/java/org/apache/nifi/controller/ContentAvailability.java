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
package org.apache.nifi.controller;

/**
 * Provides information about whether or not the data referenced in a Provenance
 * Event can be replayed or downloaded
 */
public interface ContentAvailability {

    /**
     * @return a boolean indicating whether or not the Input content is
     * available
     */
    boolean isInputAvailable();

    /**
     * @return a boolean indicating whether or not the Output content is
     * available
     */
    boolean isOutputAvailable();

    /**
     * @return <code>true</code> if the Input content is the same as the Output
     * content
     */
    boolean isContentSame();

    /**
     * @return a boolean indicating whether or not the content is replayable. If
     * this returns <code>false</code>, the reason that replay is not available
     * can be determined by calling {@link #getReasonNotReplayable()}
     */
    boolean isReplayable();

    /**
     * @return the reason that the content cannot be replayed, or
     * <code>null</code> if the content can be replayed
     */
    String getReasonNotReplayable();
}
