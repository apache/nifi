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
package org.apache.nifi.cdc.mysql.event.io;

import org.apache.nifi.cdc.mysql.event.BinlogTableEventInfo;

import java.io.IOException;
import java.io.Serializable;

/**
 * An abstract base class for writing MYSQL table-related binlog events into flow file(s), e.g.
 */
public abstract class AbstractBinlogTableEventWriter<T extends BinlogTableEventInfo> extends AbstractBinlogEventWriter<T> {

    protected Object getWritableObject(Integer type, Serializable value) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else if (value instanceof Number) {
                return value;
            } else {
                return null;
            }
        } else {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else if (value instanceof Number) {
                return value;
            } else {
                return value.toString();
            }
        }
    }

    protected void writeJson(T event) throws IOException {
        super.writeJson(event);
        if (event.getDatabaseName() != null) {
            jsonGenerator.writeStringField("database", event.getDatabaseName());
        } else {
            jsonGenerator.writeNullField("database");
        }
        if (event.getTableName() != null) {
            jsonGenerator.writeStringField("table_name", event.getTableName());
        } else {
            jsonGenerator.writeNullField("table_name");
        }
        if (event.getTableId() != null) {
            jsonGenerator.writeNumberField("table_id", event.getTableId());
        } else {
            jsonGenerator.writeNullField("table_id");
        }
    }
}
