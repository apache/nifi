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

package org.apache.nifi.service.cassandra.mapping;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A CQL {@code time} codec that binds {@code java.sql.Time} (what {@code DataTypeUtils} produces for a
 * TIME-typed record field) by converting to and from {@link LocalTime} in the system default time zone.
 */
public class JavaSQLTimeCodec implements TypeCodec<Time> {
    private final TypeCodec<LocalTime> localTimeCodec;

    public JavaSQLTimeCodec() {
        this.localTimeCodec = TypeCodecs.TIME;
    }

    @Override
    public GenericType<Time> getJavaType() {
        return GenericType.of(Time.class);
    }

    @Override
    public DataType getCqlType() {
        return localTimeCodec.getCqlType(); // maps to CQL `time`
    }

    @Override
    public ByteBuffer encode(Time value, ProtocolVersion protocolVersion) {
        return value == null ? null : localTimeCodec.encode(value.toLocalTime(), protocolVersion);
    }

    @Override
    public Time decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final LocalTime localTime = localTimeCodec.decode(bytes, protocolVersion);
        return localTime == null ? null : toSqlTime(localTime);
    }

    @Override
    public String format(Time value) {
        return value == null ? "NULL" : localTimeCodec.format(value.toLocalTime());
    }

    @Override
    public Time parse(String value) {
        final LocalTime localTime = localTimeCodec.parse(value);
        return localTime == null ? null : toSqlTime(localTime);
    }

    private static Time toSqlTime(final LocalTime localTime) {
        final ZonedDateTime zonedDateTime = localTime.atDate(LocalDate.ofEpochDay(0)).atZone(ZoneId.systemDefault());
        return new Time(zonedDateTime.toInstant().toEpochMilli());
    }
}
