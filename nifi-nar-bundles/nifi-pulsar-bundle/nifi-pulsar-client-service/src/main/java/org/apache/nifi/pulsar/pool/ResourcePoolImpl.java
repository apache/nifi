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
package org.apache.nifi.pulsar.pool;

import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ResourcePoolImpl<R extends PoolableResource> implements ResourcePool<R> {

    private final Lock lock = new ReentrantLock();
    private final Condition poolAvailable = lock.newCondition();
    private int max_resources;
    private final Vector<R> pool;

    private final ResourceExceptionHandler resourceExceptionHandler;
    private final ResourceFactory<R> factory;

    public ResourcePoolImpl(ResourceFactory<R> factory, int max_resources) {
            this(factory, new ResourceExceptionHandlerImpl(), max_resources);
    }

    public ResourcePoolImpl(ResourceFactory<R> factory, ResourceExceptionHandler handler, int max_resources) {
        lock.lock();
        try {
            this.factory = factory;
            this.resourceExceptionHandler = handler;
            this.max_resources = max_resources;
            this.pool = new Vector<R>(max_resources);
        } finally {
            lock.unlock();
        }
    }

    private R createResource(Properties props) {
        R resource = null;
        try {
            resource = factory.create(props);
            if (resource == null) {
                throw new ResourceCreationException("Unable to create resource");
            }

        } catch (Exception e) {
            resourceExceptionHandler.handle(e);
        }
        return resource;
    }


    /*
     * Shutdown the pool and release the resources
     */
    public void close() {

        Iterator<R> itr = pool.iterator();
        while (itr.hasNext()) {
            itr.next().close();
        }

    }

    public boolean isEmpty() {
            return (pool.isEmpty());
    }

    public boolean isFull() {
        return (pool != null && pool.size() == max_resources);
    }

    @Override
    public R acquire(Properties props) throws InterruptedException {
        lock.lock();
        try {
            while (max_resources <= 0) {
                poolAvailable.await();
            }

            --max_resources;

            if (pool != null) {
                int size = pool.size();
                if (size > 0)
                    return pool.remove(size - 1);
            }
            return createResource(props);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void evict(R resource) {
        lock.lock();
        try {

            // Attempt to close the connection
            if (!resource.isClosed()) {
                resource.close();
            }

            pool.removeElement(resource);
            --max_resources;
            poolAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(R resource) {
        lock.lock();
        try {
            pool.addElement(resource);
            ++max_resources;
            poolAvailable.signal();
        } finally {
            lock.unlock();
        }
    }
}
