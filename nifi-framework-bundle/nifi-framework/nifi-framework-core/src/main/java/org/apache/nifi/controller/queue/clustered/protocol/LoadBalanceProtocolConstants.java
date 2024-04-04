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

package org.apache.nifi.controller.queue.clustered.protocol;

public class LoadBalanceProtocolConstants {
    // Protocol negotiation constants
    public static final int VERSION_ACCEPTED = 0x10;
    public static final int REQEUST_DIFFERENT_VERSION = 0x11;
    public static final int ABORT_PROTOCOL_NEGOTIATION = 0x12;

    // Transaction constants
    public static final int CONFIRM_CHECKSUM = 0x21;
    public static final int REJECT_CHECKSUM = 0x22;
    public static final int COMPLETE_TRANSACTION = 0x23;
    public static final int ABORT_TRANSACTION = 0x24;
    public static final int CONFIRM_COMPLETE_TRANSACTION = 0x25;

    // FlowFile constants
    public static final int MORE_FLOWFILES = 0x31;
    public static final int NO_MORE_FLOWFILES = 0x32;

    // Backpressure / Space constants
    public static final int CHECK_SPACE = 0x61;
    public static final int SKIP_SPACE_CHECK = 0x62;
    public static final int SPACE_AVAILABLE = 0x65;
    public static final int QUEUE_FULL = 0x66;

    // data frame constants
    public static final int NO_DATA_FRAME = 0x40;
    public static final int DATA_FRAME_FOLLOWS = 0x42;
}
