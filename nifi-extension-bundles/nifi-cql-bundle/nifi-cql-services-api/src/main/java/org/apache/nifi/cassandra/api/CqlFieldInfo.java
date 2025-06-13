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

package org.apache.nifi.cassandra.api;

public class CqlFieldInfo {
    private String fieldName;
    private String dataType;
    private int dataTypeProtocolCode;

    public CqlFieldInfo(String fieldName, String dataType, int dataTypeProtocolCode) {
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.dataTypeProtocolCode = dataTypeProtocolCode;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getDataType() {
        return dataType;
    }

    public int getDataTypeProtocolCode() {
        return dataTypeProtocolCode;
    }
}