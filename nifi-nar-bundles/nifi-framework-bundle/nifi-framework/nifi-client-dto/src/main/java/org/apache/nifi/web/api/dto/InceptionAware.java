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
package org.apache.nifi.web.api.dto;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Random;
import java.util.UUID;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "inception")
class InceptionAware implements Comparable<InceptionAware> {

    private volatile UUID inceptionId;

    @ApiModelProperty(value = "The inception id of the component.")
    public synchronized String getInceptionId() {
        return this._getInceptionId().toString();
    }

    /**
     *
     * @return
     */
    public synchronized UUID _getInceptionId() {
        if (this.inceptionId == null){
            this.inceptionId = UUID1Generator.generateId();
        }
        return this.inceptionId;
    }

    /**
     *
     */
    public void setInceptionId(String inceptionId) {
        this.inceptionId = UUID.fromString(inceptionId);
    }

    /**
     *
     */
    private static class UUID1Generator {

        public static final Object lock = new Object();

        private static long lastTime;
        private static long clockSequence = 0;
        private static final long hostIdentifier = getHostId();

        /**
         * Will generate unique time based UUID where the next UUID is always
         * greater then the previous.
         */
        public final static UUID generateId() {
            return generateIdFromTimestamp(System.currentTimeMillis());
        }

        /**
         *
         */
        public final static UUID generateIdFromTimestamp(long currentTimeMillis) {
            long time;

            synchronized (lock) {
                if (currentTimeMillis > lastTime) {
                    lastTime = currentTimeMillis;
                    clockSequence = 0;
                } else {
                    ++clockSequence;
                }
            }

            time = currentTimeMillis;

            // low Time
            time = currentTimeMillis << 32;

            // mid Time
            time |= ((currentTimeMillis & 0xFFFF00000000L) >> 16);

            // hi Time
            time |= 0x1000 | ((currentTimeMillis >> 48) & 0x0FFF);

            long clockSequenceHi = clockSequence;

            clockSequenceHi <<= 48;

            long lsb = clockSequenceHi | hostIdentifier;

            return new UUID(time, lsb);
        }

        /**
         *
         */
        private static final long getHostId() {
            long macAddressAsLong = 0;
            try {
                Random random = new Random();
                InetAddress address = InetAddress.getLocalHost();
                NetworkInterface ni = NetworkInterface.getByInetAddress(address);
                if (ni != null) {
                    byte[] mac = ni.getHardwareAddress();
                    random.nextBytes(mac); // we don't really want to reveal the
                                           // actual MAC address
                    // Converts array of unsigned bytes to an long
                    if (mac != null) {
                        for (int i = 0; i < mac.length; i++) {
                            macAddressAsLong <<= 8;
                            macAddressAsLong ^= (long) mac[i] & 0xFF;
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return macAddressAsLong;
        }
    }

    @Override
    public int compareTo(InceptionAware that) {
        return this.getInceptionId().compareTo(that.getInceptionId());
    }
}
