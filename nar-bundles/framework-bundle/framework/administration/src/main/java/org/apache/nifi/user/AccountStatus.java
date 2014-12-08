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
package org.apache.nifi.user;

/**
 * Represents the status of a user's account.
 */
public enum AccountStatus {

    ACTIVE,
    PENDING,
    DISABLED;

    /**
     * Returns the matching status or null if the specified status does not
     * match any statuses.
     *
     * @param rawStatus
     * @return
     */
    public static AccountStatus valueOfStatus(String rawStatus) {
        AccountStatus desiredStatus = null;

        for (AccountStatus status : values()) {
            if (status.toString().equals(rawStatus)) {
                desiredStatus = status;
                break;
            }
        }

        return desiredStatus;
    }
}
