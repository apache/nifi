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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractComponentScheduler implements ComponentScheduler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractComponentScheduler.class);

    private final AtomicLong pauseCount = new AtomicLong(0L);
    private final Queue<Connectable> toStart = new LinkedBlockingQueue<>();

    @Override
    public void pause() {
        final long count = pauseCount.incrementAndGet();
        logger.debug("{} paused; count = {}", this, count);
    }

    @Override
    public void resume() {
        final long updatedCount = pauseCount.decrementAndGet();
        logger.debug("{} resumed; count = {}", this, updatedCount);

        if (updatedCount > 0) {
            return;
        }

        Connectable connectable;
        while ((connectable = toStart.poll()) != null) {
            logger.debug("{} starting {}", this, connectable);
            startNow(connectable);
        }
    }

    private boolean isPaused() {
        return pauseCount.get() > 0;
    }

    @Override
    public void startComponent(final Connectable component) {
        if (isPaused()) {
            logger.debug("{} called to start {} but paused so will queue it for start later", this, component);
            toStart.offer(component);
        } else {
            logger.debug("{} starting {} now", this, component);
            startNow(component);
        }
    }

    protected abstract void startNow(Connectable component);
}
