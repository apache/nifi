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

public enum IngestionReportLevelEnum {
    IRL_NONE("IngestionReportLevel:None","No reports are generated at all."),
    IRL_FAIL("IngestionReportLevel:Failure","Status get's reported on failure only."),
    IRL_FAS("IngestionReportLevel:FailureAndSuccess","Status get's reported on failure and success.");

    private String ingestionReportLevel;

    private String description;


    IngestionReportLevelEnum(String displayName, String description) {
        this.ingestionReportLevel = displayName;
        this.description = description;
    }

    public String getIngestionReportLevel() {
        return ingestionReportLevel;
    }

    public String getDescription() {
        return description;
    }
}
