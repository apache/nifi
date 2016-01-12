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
package org.apache.nifi.remote;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.protocol.DataPacket;

/**
 * A TransactionCompletion provides information about a {@link Transaction} that
 * has completed successfully.
 */
public interface TransactionCompletion {

    /**
     * When a sending to a NiFi instance, the server may accept the content sent
     * to it but indicate that its queues are full and that the client should
     * backoff sending data for a bit.
     *
     * @return <code>true</code> if the server did in fact request that,
     * <code>false</code> otherwise
     */
    boolean isBackoff();

    /**
     * @return the number of Data Packets that were sent to or received from the
     * remote NiFi instance in the Transaction
     */
    int getDataPacketsTransferred();

    /**
     * @return the number of bytes of DataPacket content that were sent to or
     * received from the remote NiFI instance in the Transaction. Note that this
     * is different than the number of bytes actually transferred between the
     * client and server, as it does not take into account the attributes or
     * protocol-specific information that is exchanged but rather takes into
     * account only the data in the {@link InputStream} of the
     * {@link DataPacket}
     */
    long getBytesTransferred();

    /**
     * @param timeUnit unit of time for which to report the duration
     * @return the amount of time that the Transaction took, from the time that
     * the Transaction was created to the time that the Transaction was
     * completed
     */
    long getDuration(TimeUnit timeUnit);
}
