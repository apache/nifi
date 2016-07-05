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

package org.apache.nifi.web.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface DistributedLock {

    /**
     * Obtains a lock, blocking as long as necessary to obtain the lock.
     * Once a lock has been obtained, the identifier of the version of the lock is returned,
     * which can be passed to the {@link #withLock(String, Callable)} or
     * {@link #unlock(String)} method. Once this method returns, it is
     * important that either the {@link #withLock(String, Callable)} or
     * {@link #unlock(String)} method be called with this identifier. Otherwise,
     * any attempt to claim another read lock or write lock will block until this
     * lock expires.
     *
     * @return the identifier
     */
    String lock();

    /**
     * Obtains a lock, blocking as long as necessary to obtain the lock.
     * Once a lock has been obtained, the identifier of the version of the lock is returned,
     * which can be passed to the {@link #withLock(String, Callable)} or
     * {@link #unlock(String)} method. Once this method returns, it is
     * important that either the {@link #withLock(String, Callable)} or
     * {@link #unlock(String)} method be called with this identifier. Otherwise,
     * any attempt to claim another read lock or write lock will block until this
     * lock expires.
     *
     * @param versionIdentifier a value that should be used as the version id instead of generating one.
     *            This allows us to ensure that all nodes in the cluster use the same id.
     *
     * @return the identifier
     */
    String lock(String versionIdentifier);

    /**
     * Waits up to the given amount of time to obtain a lock. If the lock is obtained
     * within this time period, the identifier will be returned, as with {@link #lock()}.
     * If the lock cannot be obtained within the given time period, <code>null</code> will
     * be returned.
     *
     * @param time the maximum amount of time to wait for the lock
     * @param timeUnit the unit of time that the time parameter is in
     * @return the identifier of the lock, or <code>null</code> if no lock is obtained
     */
    String tryLock(long time, TimeUnit timeUnit);

    /**
     * Waits up to the given amount of time to obtain a lock. If the lock is obtained
     * within this time period, the identifier will be returned, as with {@link #lock()}.
     * If the lock cannot be obtained within the given time period, <code>null</code> will
     * be returned.
     *
     * @param time the maximum amount of time to wait for the lock
     * @param timeUnit the unit of time that the time parameter is in
     * @param versionIdentifier a value that should be used as the version id instead of generating one.
     *            This allows us to ensure that all nodes in the cluster use the same id.
     * @return the identifier of the lock, or <code>null</code> if no lock is obtained
     */
    String tryLock(long time, TimeUnit timeUnit, String versionIdentifier);

    /**
     * Performs the given action while this lock is held. The identifier of the lock that was
     * obtained by calling {@link #lock()} must be provided to this method. If the
     * lock identifier is incorrect, or the lock has expired, a {@link LockExpiredException}
     * will be thrown. This method provides a mechanism for verifying that the lock obtained
     * by {@link #lock()} or {@link #tryLock(long, TimeUnit)} is still valid and that the action
     * being performed will be done so without the lock expiring (i.e., if the lock expires while
     * the action is being performed, the lock won't be released until the provided action completes).
     *
     * @param identifier the identifier of the lock that has already been obtained
     * @param action the action to perform
     *
     * @return the value returned by the given action
     *
     * @throws LockExpiredException if the provided identifier is not the identifier of the currently
     *             held lock, or if the lock that was obtained has already expired and is no longer valid
     */
    <T> T withLock(String identifier, Supplier<T> action) throws LockExpiredException;

    /**
     * Cancels the lock with the given identifier, so that the lock is no longer valid.
     *
     * @param identifier the identifier of the lock that was obtained by calling {@link #lock()}.
     *
     * @throws LockExpiredException if the provided identifier is not the identifier of the currently
     *             held lock, or if the lock that was obtained has already expired and is no longer valid
     */
    void unlock(String identifier) throws LockExpiredException;
}
