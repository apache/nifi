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
package org.apache.nifi.remote.protocol.socket;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.TransactionCompletion;

public class SocketClientTransactionCompletion implements TransactionCompletion {

    private final boolean backoff;
    private final int dataPacketsTransferred;
    private final long bytesTransferred;
    private final long durationNanos;

    public SocketClientTransactionCompletion(final boolean backoff, final int dataPacketsTransferred, final long bytesTransferred, final long durationNanos) {
        this.backoff = backoff;
        this.dataPacketsTransferred = dataPacketsTransferred;
        this.bytesTransferred = bytesTransferred;
        this.durationNanos = durationNanos;
    }

    @Override
    public boolean isBackoff() {
        return backoff;
    }

    @Override
    public int getDataPacketsTransferred() {
        return dataPacketsTransferred;
    }

    @Override
    public long getBytesTransferred() {
        return bytesTransferred;
    }

    @Override
    public long getDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(durationNanos, TimeUnit.NANOSECONDS);
    }

}
