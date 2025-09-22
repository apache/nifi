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
package org.apache.nifi.events;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.nifi.reporting.Bulletin;

/**
 *
 */
public class NodeBulletinProcessingStrategy implements BulletinProcessingStrategy {
    static final int MAX_ENTRIES = 5;
    private final Queue<Bulletin> ringBuffer = new CircularFifoQueue<>(MAX_ENTRIES);

    @Override
    public synchronized void update(final Bulletin bulletin) {
        ringBuffer.add(bulletin);
    }

    public synchronized Set<Bulletin> getBulletins() {
        final Set<Bulletin> response = new HashSet<>(ringBuffer);
        ringBuffer.clear();
        return response;
    }
}
