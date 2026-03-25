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
package org.apache.nifi.registry.cluster;

import java.util.List;
import java.util.Map;

/**
 * Handles inter-node HTTP communication for write replication.
 *
 * <p>Write routing uses HTTP 307 redirects rather than server-side proxying.
 * When a follower receives a write it returns 307 with a {@code Location} header
 * pointing to the leader so the client re-sends its request directly, preserving
 * any TLS/mTLS client identity end-to-end.
 *
 * <p>This interface covers only the leader-to-follower fan-out direction, which
 * uses the shared internal auth token and does not carry client credentials.
 */
public interface ReplicationClient {

    /**
     * Fans out the write described by {@code path}, {@code method},
     * {@code headers}, and {@code body} to each node in {@code followers}.
     *
     * <p>Each follower is contacted asynchronously on a background thread.
     * Failures are logged but do not block the caller; the leader's response
     * has already been sent to the client before this method is called.
     *
     * @param path      request path including any query string
     *                  (e.g. {@code /nifi-registry-api/buckets?param=value})
     * @param method    HTTP method (e.g. {@code POST}, {@code DELETE})
     * @param headers   original request headers to forward (hop-by-hop headers
     *                  are filtered out by the implementation)
     * @param body      request body bytes (may be empty)
     * @param followers destination nodes; must not include this node
     */
    void replicateToFollowers(String path, String method,
                               Map<String, String> headers, byte[] body,
                               List<NodeAddress> followers);
}
