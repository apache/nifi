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
package org.apache.nifi.dto.splunk;

/**
 * {@inheritDoc}
 *
 * This response object is used when sending data is successful. Contains additional information.
 */
public class SendRawDataSuccessResponse extends SendRawDataResponse {
    private long ackId;

    public long getAckId() {
        return ackId;
    }

    public void setAckId(final long ackId) {
        this.ackId = ackId;
    }
}
