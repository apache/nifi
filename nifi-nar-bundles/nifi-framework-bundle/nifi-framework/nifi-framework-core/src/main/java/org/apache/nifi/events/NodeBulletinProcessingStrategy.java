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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.reporting.Bulletin;

/**
 * Strategy should not hold more than 5 Bulletins in memory.
 */
public class NodeBulletinProcessingStrategy implements BulletinProcessingStrategy {

    protected static final int MAX_ENTRIES = 5;
    private final Lock lock;
    private final Set<Bulletin> bulletins;

    public NodeBulletinProcessingStrategy() {
        lock = new ReentrantLock();
        bulletins = Collections.newSetFromMap(new LinkedHashMap<Bulletin, Boolean>(){
            private static final long serialVersionUID = 1L;

            protected boolean removeEldestEntry(Map.Entry<Bulletin, Boolean> eldest) {
                return size() > MAX_ENTRIES;
            }
        });
    }

    @Override
    public void update(final Bulletin bulletin) {
        lock.lock();
        try {
            bulletins.add(bulletin);
        } finally {
            lock.unlock();
        }
    }

    public Set<Bulletin> getBulletins() {
        final Set<Bulletin> response = new HashSet<>();

        lock.lock();
        try {
            // get all the bulletins currently stored
            response.addAll(bulletins);

            // remove the bulletins
            bulletins.clear();
        } finally {
            lock.unlock();
        }

        return response;
    }
}
