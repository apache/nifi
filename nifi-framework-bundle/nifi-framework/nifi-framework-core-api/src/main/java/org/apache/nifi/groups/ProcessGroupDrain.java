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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.queue.QueueSize;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 * A drain of a Process Group. While the drain is {@link ProcessGroupDrainState#DRAINING DRAINING}, the sources of the group are kept stopped and
 * everything else is kept running so that the queued data can flow out. The sources are Processors with no incoming connection from another
 * component, Input Ports of the drained group itself, Remote Process Group Output Ports, and Stateless child groups that contain a source Processor.
 * </p>
 *
 * <p>
 * A drain that has failed or ended no longer restricts any component, but the Process Group keeps it until a new drain replaces it so that its
 * outcome can be reported. Drains are held in memory only.
 * </p>
 */
public interface ProcessGroupDrain {

    String getIdentifier();

    ProcessGroup getProcessGroup();

    ProcessGroupDrainState getState();

    Optional<String> getFailureReason();

    /**
     * @return true once every source has stopped and the other components have been asked to start, or once preparing the drain has failed.
     *         A source that is asked to stop may still bring data into the group until it has stopped.
     */
    boolean isPrepared();

    /**
     * @return a future that completes when the drain is {@link #isPrepared() prepared}
     */
    CompletableFuture<Void> getPreparationFuture();

    /**
     * @return the amount of data that was queued once the sources had stopped, or the amount queued now if they have not stopped yet
     */
    QueueSize getOriginalQueueSize();

    QueueSize getCurrentQueueSize();

    /**
     * @return the components that have data queued but cannot process it because they are disabled or invalid
     */
    List<DrainBlockingComponent> getBlockingComponents();

    boolean keepsStopped(Connectable connectable);

    boolean keepsRunning(Connectable connectable);

    boolean keepsStopped(ProcessGroup statelessGroup);

    /**
     * @return true if the drain keeps the given Stateless group running. A Stateless group that contains no source and cannot receive data
     *         through an Input Port is neither kept stopped nor kept running.
     */
    boolean keepsRunning(ProcessGroup statelessGroup);

    /**
     * @throws IllegalStateException if the drain keeps the component stopped and the drained group still has data queued
     */
    void verifyCanStart(Connectable connectable);

    /**
     * @throws IllegalStateException if the drain keeps the component running and the drained group still has data queued
     */
    void verifyCanStop(Connectable connectable);

    /**
     * @throws IllegalStateException if the drain keeps the group stopped and the drained group still has data queued
     */
    void verifyCanStart(ProcessGroup statelessGroup);

    /**
     * @throws IllegalStateException if the drain keeps the group running and the drained group still has data queued
     */
    void verifyCanStop(ProcessGroup statelessGroup);
}
