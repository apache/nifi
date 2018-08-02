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
package org.apache.nifi.controller.repository;

import java.io.Closeable;
import java.io.IOException;

public interface FlowFileEventRepository extends Closeable {

    /**
     * Updates the repository to include a new FlowFile processing event
     *
     * @param event new event
     * @param  componentIdentifier the ID of the component that the event belongs to
     * @throws java.io.IOException ioe
     */
    void updateRepository(FlowFileEvent event, String componentIdentifier) throws IOException;

    /**
     * @param now the current time
     * @return a report of processing activity since the given time
     */
    RepositoryStatusReport reportTransferEvents(long now);

    /**
     * Causes any flow file events of the given entry age in epoch milliseconds
     * or older to be purged from the repository
     *
     * @param cutoffEpochMilliseconds cutoff
     */
    void purgeTransferEvents(long cutoffEpochMilliseconds);

    /**
     * Causes any flow file events of the given component to be purged from the
     * repository
     *
     * @param componentIdentifier Identifier of the component
     */
    void purgeTransferEvents(String componentIdentifier);
}
