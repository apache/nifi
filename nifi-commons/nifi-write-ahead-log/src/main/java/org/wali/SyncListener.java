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
package org.wali;

/**
 * <p>
 * Provides a callback mechanism by which applicable listeners can be notified
 * when a WriteAheadRepository is synched (via the
 * {@link WriteAheadRepository#sync()} method) or one of its partitions is
 * synched via
 * {@link WriteAheadRepository#update(java.util.Collection, boolean)} with a
 * value of <code>true</code> for the second argument.
 * </p>
 *
 * <p>
 * It is not required that an implementation of {@link WriteAheadRepository}
 * support this interface. Those that do generally will require that the
 * listener be injected via the constructor.
 * </p>
 *
 * <p>
 * All implementations of this interface must be thread-safe.
 * </p>
 *
 * <p>
 * The {@link #onSync(int)} method will always be called while the associated
 * partition is locked. The {@link #onGlobalSync()} will always be called while
 * the entire repository is locked.
 * </p>
 *
 */
public interface SyncListener {

    /**
     * This method is called whenever a specific partition is synched via the
     * {@link WriteAheadRepository#update(java.util.Collection, boolean)} method
     *
     * @param partitionIndex the index of the partition that was synched
     */
    void onSync(int partitionIndex);

    /**
     * This method is called whenever the entire
     * <code>WriteAheadRepository</code> is synched via the
     * {@link WriteAheadRepository#sync()} method.
     */
    void onGlobalSync();
}
