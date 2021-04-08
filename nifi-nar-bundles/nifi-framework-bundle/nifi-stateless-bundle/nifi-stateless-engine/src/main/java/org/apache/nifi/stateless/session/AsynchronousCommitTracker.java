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
import org.apache.nifi.connectable.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Stack;
import java.util.function.Consumer;

/**
 * Simple component used to track which Connectables are ready to be triggered
 */
public class AsynchronousCommitTracker {
    private static final Logger logger = LoggerFactory.getLogger(AsynchronousCommitTracker.class);

    private final Set<Connectable> ready = new LinkedHashSet<>();
    private final Set<Connectable> readOnly = Collections.unmodifiableSet(ready);
    private final Stack<CommitCallbacks> commitCallbacks = new Stack<>();

    public void addConnectable(final Connectable connectable) {
        ready.add(connectable);
    }

    public Set<Connectable> getReady() {
        return readOnly;
    }

    public boolean isAnyReady() {
        return !ready.isEmpty();
    }

    public boolean isReady(final Connectable connectable) {
        if (!ready.contains(connectable)) {
            return false;
        }

        for (final Connection incoming : connectable.getIncomingConnections()) {
            if (!incoming.getFlowFileQueue().isEmpty()) {
                return true;
            }
        }

        ready.remove(connectable);
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
