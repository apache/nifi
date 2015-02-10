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
package org.apache.nifi.remote.exception;

/**
 * PortNotRunningException occurs when the remote NiFi instance reports
 * that the Port that the client is attempting to communicate with is not
 * currently running and therefore communications with that Port are not allowed.
 */
public class PortNotRunningException extends ProtocolException {
    private static final long serialVersionUID = -2790940982005516375L;

    public PortNotRunningException(final String message) {
        super(message);
    }
}
