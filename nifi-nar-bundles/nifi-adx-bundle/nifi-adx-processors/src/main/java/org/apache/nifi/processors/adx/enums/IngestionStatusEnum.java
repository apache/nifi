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
package org.apache.nifi.processors.adx.enums;

public enum IngestionStatusEnum {
    ST_SUCCESS("IngestionStatus:SUCCESS","Wait until ingestions are reported as succeeded."),
    ST_FIREANDFORGET("IngestionStatus:FIRE_AND_FORGET","Do not wait on ADX for status.");

    private String ingestionStatus;
    private String description;


    IngestionStatusEnum(String ingestionStatus, String description) {
        this.ingestionStatus = ingestionStatus;
        this.description = description;
    }

    public String getIngestionStatus() {
        return ingestionStatus;
    }

    public String getDescription() {
        return description;
    }
}
