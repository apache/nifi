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

/**
 * <p>
 * A DistributedLockingManager is responsible for exposing a mechanism that
 * clients can use to obtain a lock on the dataflow.
 * </p>
 *
 * <p>
 * Because of the way in which NiFi replicates requests from one node to all
 * other nodes in the cluster, it is important that all nodes in the cluster
 * are able to obtain a lock for the request before the request is allowed to
 * proceed. This is accomplished by using a two-phase approach. For each request
 * that will require a lock (either a read (shared) lock or a write (mutually
 * exclusive) lock), the request must be done in two phases. The first phase is
 * responsible for obtaining the lock and optionally performing validation of
 * the request. Once a node has obtained the necessary lock and performed any
 * required validation, the node will respond to the web request with a status
 * code of 150 - NodeContinue.
 * </p>
 *
 * <p>
 * At this point, the node that originated the request
 * will verify that either all nodes obtained a lock or that at least one node
 * failed to obtain a lock. If all nodes respond with a 150 - NodeContinue,
 * then the second phase of the request will occur. In the second phase, the
 * actual logic of the desired request is performed while the lock is held.
 * The lock is then released, once the logic is performed (or if the logic fails
 * to be performed).
 * </p>
 *
 * <p>
 * In the case that at least one node responds with a status code with than
 * 150 - NodeContinue, the node that originated the request will instead issue
 * a cancel request for the second phase so that all nodes are able to unlock
 * the lock that was previously obtained for the request.
 * </p>
 *
 * <p>
 * A key consideration in this type of approach that must be taken into account
 * is that the node that originated the request could, at any point in time, fail
 * as a result of the process being killed, power loss, network connectivity problems,
 * etc. As a result, the locks that are obtained through a DistributedLockingManager
 * are designed to expire after some amount of time, so that locks are not held
 * indefinitely, even in the case of node failure.
 * </p>
 */
public interface DistributedLockingManager {

    DistributedLock getReadLock();

    DistributedLock getWriteLock();

}
