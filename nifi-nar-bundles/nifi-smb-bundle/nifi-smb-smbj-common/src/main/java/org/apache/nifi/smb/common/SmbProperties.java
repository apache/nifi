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
package org.apache.nifi.smb.common;

import org.apache.nifi.components.PropertyDescriptor;

import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

public class SmbProperties {

    public static final PropertyDescriptor SMB_DIALECT = new PropertyDescriptor.Builder()
            .name("smb-dialect")
            .displayName("SMB Dialect")
            .description("The SMB dialect is negotiated between the client and the server by default to the highest common version supported by both end. " +
                    "In some rare cases, the client-server communication may fail with the automatically negotiated dialect. This property can be used to set the dialect explicitly " +
                    "(e.g. to downgrade to a lower version), when those situations would occur.")
            .required(true)
            .allowableValues(SmbDialect.class)
            .defaultValue(SmbDialect.AUTO.getValue())
            .build();

    public static final PropertyDescriptor USE_ENCRYPTION = new PropertyDescriptor.Builder()
            .name("use-encryption")
            .displayName("Use Encryption")
            .description("Turns on/off encrypted communication between the client and the server. The property's behavior is SMB dialect dependent: " +
                    "SMB 2.x does not support encryption and the property has no effect. " +
                    "In case of SMB 3.x, it is a hint/request to the server to turn encryption on if the server also supports it.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor ENABLE_DFS = new PropertyDescriptor.Builder()
            .name("enable-dfs")
            .displayName("Enable DFS")
            .description("Enables accessing Distributed File System (DFS) and following DFS links during SMB operations.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Timeout")
            .name("timeout")
            .description("Timeout for read and write operations.")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(TIME_PERIOD_VALIDATOR)
            .build();
}
