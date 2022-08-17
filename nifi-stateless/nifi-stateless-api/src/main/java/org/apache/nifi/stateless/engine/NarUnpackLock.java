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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * If multiple Stateless dataflows are loaded concurrently within the same JVM, we need to ensure that the dataflows
 * do not stomp on one another when unpacking NAR's. To do that, we need a mechanism by which a single lock can be shared
 * across multiple classes, as the Extension Repository as well as the bootstrap logic may attempt to unpack NARs.
 * Because these classes exist across multiple modules, and because statically defined locks at that level may not be enough
 * (due to multiple classloders being used for the 'stateless nar'), we define a singleton Lock within the nifi-stateless-api module.
 * This lock should always be obtained before attempting to unpack nars.
 */
public class NarUnpackLock {
    private static final Logger logger = LoggerFactory.getLogger(NarUnpackLock.class);

    private static final Lock lock = new ReentrantLock();

    public static void lock() {
        lock.lock();
        logger.debug("Lock obtained by thread {}: {}", Thread.currentThread().getId(), Thread.currentThread().getName());
    }

    public static void unlock() {
        lock.unlock();
        logger.debug("Lock obtained by thread {}: {}", Thread.currentThread().getId(), Thread.currentThread().getName());
    }

    public static boolean tryLock() {
        final boolean obtained = lock.tryLock();

        if (obtained) {
            logger.debug("Lock obtained by thread {}: {}", Thread.currentThread().getId(), Thread.currentThread().getName());
        }

        return obtained;
    }
}
