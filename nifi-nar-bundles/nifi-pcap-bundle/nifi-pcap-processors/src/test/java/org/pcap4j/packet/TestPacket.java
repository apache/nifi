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
package org.pcap4j.packet;

/**
 * A test Packet implementation that makes SimplePacket public.
 */
public class TestPacket extends SimplePacket {

    public TestPacket(byte[] rawData, int offset, int length) {
       super(rawData, offset, length);
    }

    @Override
    protected String modifier() {
        return null;
    }

    @Override
    public Packet.Builder getBuilder() {
        return null;
    }

    @Override
    public Packet getPayload() {
        return new TestPacket(this.getRawData(), 0, this.getRawData().length);
    }

}
