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

package org.apache.nifi.stateless.session;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.groups.ProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.function.Consumer;

/**
 * Simple component used to track which Connectables are ready to be triggered
 */
public class AsynchronousCommitTracker {
    private static final Logger logger = LoggerFactory.getLogger(AsynchronousCommitTracker.class);

    private final Set<Connectable> ready = new LinkedHashSet<>();
    private final Stack<CommitCallbacks> commitCallbacks = new Stack<>();
    private int flowFilesProduced = 0;
    private long bytesProduced = 0L;
    private boolean progressMade = false;

    public void addConnectable(final Connectable connectable) {
        // this.ready is a LinkedHashSet that is responsible for ensuring that when a Connectable is added,
        // it will be the first to be triggered. What we really want is to insert the new Connectable at the front
        // of the collection, regardless of whether it's currently present or not. However, using a List or a Queue
        // is not ideal because checking for the existence of the Connectable in a List or Queue is generally quite expensive,
        // even though the insertion is cheap. To achieve the desired behavior, we call remove() and then add(), which ensures
        // that the given Connectables goes to the END of the list. When getReady() is called, the LinkedHashSet is then
        // copied into a List and reversed. There is almost certainly a much more efficient way to achieve this, but that
        // is an optimization best left for a later date.
        final boolean removed = ready.remove(connectable);
        ready.add(connectable);

        if (removed) {
            logger.debug("{} Added {} to list of Ready Connectables but it was already in the list", this, connectable);
        } else {
            logger.debug("{} Added {} to list of Ready Connectables", this, connectable);
        }
    }

    public List<Connectable> getReady() {
        final List<Connectable> connectables = new ArrayList<>(ready);
        Collections.reverse(connectables);
        return connectables;
    }

    public boolean isAnyReady() {
        final boolean anyReady = !ready.isEmpty();

        logger.debug("{} Any components ready = {}, list={}", this, anyReady, ready);
        return anyReady;
    }

    public boolean isReady(final Connectable connectable) {
        if (!ready.contains(connectable)) {
            logger.debug("{} {} is not ready because it's not in the list of ready components", this, connectable);
            return false;
        }

        if (isRootGroupOutputPort(connectable)) {
            // Output Port is at the root group level. We don't want to trigger the Output Port so we consider it not ready
            ready.remove(connectable);
            logger.debug("{} {} is not ready because it's a root group output port", this, connectable);
            return false;
        }

        if (isDataQueued(connectable)) {
            logger.debug("{} {} is ready because it has data queued", this, connectable);
            return true;
        }

        logger.debug("{} {} is not ready because it has no data queued", this, connectable);
        ready.remove(connectable);
        return false;
    }

    private boolean isRootGroupOutputPort(final Connectable connectable) {
        final ConnectableType connectableType = connectable.getConnectableType();
        if (connectableType == ConnectableType.OUTPUT_PORT) {
            final ProcessGroup outputPortGroup = connectable.getProcessGroup();
            return outputPortGroup.getParent() == null;
        }

        return false;
    }

    private boolean isDataQueued(final Connectable connectable) {
        for (final Connection incoming : connectable.getIncomingConnections()) {
            if (!incoming.getFlowFileQueue().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    public void addCallback(final Connectable connectable, final Runnable successCallback, final Consumer<Throwable> failureCallback) {
        if (successCallback == null && failureCallback == null) {
            return;
        }

        commitCallbacks.add(new CommitCallbacks(connectable, successCallback, failureCallback));
    }

    public void triggerCallbacks() {
        Throwable failure = null;

        while (!commitCallbacks.isEmpty()) {
            final CommitCallbacks callbacks = commitCallbacks.pop();
            if (failure != null) {
                handleCallbackFailure(callbacks, failure);
                continue;
            }

            try {
                triggerSuccessCallback(callbacks);
            } catch (final Throwable t) {
                logger.error("Failed to trigger onSuccess Aysnchronous Commit Callback on {}", callbacks.getConnectable(), t);
                failure = t;
            }
        }
    }

    public void triggerFailureCallbacks(final Throwable failure) {
        while (!commitCallbacks.isEmpty()) {
            final CommitCallbacks callbacks = commitCallbacks.pop();
            handleCallbackFailure(callbacks, failure);
        }
    }

    private void triggerSuccessCallback(final CommitCallbacks callbacks) {
        final Runnable callback = callbacks.getSuccessCallback();
        if (callback == null) {
            return;
        }

        callback.run();
    }

    private void handleCallbackFailure(final CommitCallbacks commitCallbacks, final Throwable failure) {
        final Consumer<Throwable> failureCallback = commitCallbacks.getFailureCallback();
        if (failureCallback == null) {
            return;
        }

        logger.debug("When triggering Asynchronous Commit callbacks, there was previously a failure so will call failure handler for {}", commitCallbacks.getConnectable());
        try {
            failureCallback.accept(failure);
        } catch (final Throwable t) {
            logger.error("Tried to invoke failure callback for asynchronous commits on {} but failed to do so", commitCallbacks.getConnectable(), t);
        }
    }

    public void recordProgress(final int flowFilesProduced, final long bytesProduced) {
        this.flowFilesProduced += flowFilesProduced;
        this.bytesProduced += bytesProduced;

        this.progressMade = true;
    }

    public void resetProgress() {
        this.flowFilesProduced = 0;
        this.bytesProduced = 0L;
        this.progressMade = false;
    }

    public boolean isProgress() {
        return progressMade;
    }

    public int getFlowFilesProduced() {
        return flowFilesProduced;
    }

    public long getBytesProduced() {
        return bytesProduced;
    }

    private static class CommitCallbacks {
        private final Connectable connectable;
        private final Runnable successCallback;
        private final Consumer<Throwable> failureCallback;

        public CommitCallbacks(final Connectable connectable, final Runnable successCallback, final Consumer<Throwable> failureCallback) {
            this.connectable = connectable;
            this.successCallback = successCallback;
            this.failureCallback = failureCallback;
        }

        public Connectable getConnectable() {
            return connectable;
        }

        public Runnable getSuccessCallback() {
            return successCallback;
        }

        public Consumer<Throwable> getFailureCallback() {
            return failureCallback;
        }
    }
}
