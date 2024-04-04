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
package org.apache.nifi.cluster.manager.exception;

/**
 * Represents the exceptional case when a HTTP request that may change a node's state is to be replicated while the cluster or connected nodes are unable to change their state (e.g., a new node is
 * connecting to the cluster).
 *
 */
public class MutableRequestException extends IllegalClusterStateException {

    public MutableRequestException() {
    }

    public MutableRequestException(String msg) {
        super(msg);
    }

    public MutableRequestException(Throwable cause) {
        super(cause);
    }

    public MutableRequestException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
