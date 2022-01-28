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

package org.apache.nifi.cluster.protocol.message;

public class CommsTimingDetails {
    private long dnsLookupMillis = -1;
    private long connectMillis = -1;
    private long sendRequestMillis = -1;
    private long receiveFirstByteMillis = -1;
    private long receiveFullResponseMillis = -1;

    public long getDnsLookupMillis() {
        return dnsLookupMillis;
    }

    public void setDnsLookupMillis(final long dnsLookupMillis) {
        this.dnsLookupMillis = dnsLookupMillis;
    }

    public long getConnectMillis() {
        return connectMillis;
    }

    public void setConnectMillis(final long connectMillis) {
        this.connectMillis = connectMillis;
    }

    public long getSendRequestMillis() {
        return sendRequestMillis;
    }

    public void setSendRequestMillis(final long sendRequestMillis) {
        this.sendRequestMillis = sendRequestMillis;
    }

    public long getReceiveFirstByteMillis() {
        return receiveFirstByteMillis;
    }

    public void setReceiveFirstByteMillis(final long receiveFirstByteMillis) {
        this.receiveFirstByteMillis = receiveFirstByteMillis;
    }

    public long getReceiveFullResponseMillis() {
        return receiveFullResponseMillis;
    }

    public void setReceiveFullResponseMillis(final long receiveFullResponseMillis) {
        this.receiveFullResponseMillis = receiveFullResponseMillis;
    }
}
