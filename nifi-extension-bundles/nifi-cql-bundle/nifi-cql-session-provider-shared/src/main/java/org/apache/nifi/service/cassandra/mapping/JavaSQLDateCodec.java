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
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * A CQL {@code date} codec that binds {@code java.sql.Date} (what {@code DataTypeUtils} produces for a
 * DATE-typed record field) by converting to and from {@link LocalDate} in the system default time zone.
 */
public class JavaSQLDateCodec implements TypeCodec<Date> {
    private final TypeCodec<LocalDate> localDateCodec;

    public JavaSQLDateCodec() {
        this.localDateCodec = TypeCodecs.DATE;
    }

    @Override
    public GenericType<Date> getJavaType() {
        return GenericType.of(Date.class);
    }

    @Override
    public DataType getCqlType() {
        return localDateCodec.getCqlType(); // maps to CQL `date`
    }

    @Override
    public ByteBuffer encode(Date value, ProtocolVersion protocolVersion) {
        return value == null ? null : localDateCodec.encode(toLocalDate(value), protocolVersion);
    }

    @Override
    public Date decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final LocalDate localDate = localDateCodec.decode(bytes, protocolVersion);
        return localDate == null ? null : toSqlDate(localDate);
    }

    @Override
    public String format(Date value) {
        return value == null ? "NULL" : localDateCodec.format(toLocalDate(value));
    }

    @Override
    public Date parse(String value) {
        final LocalDate localDate = localDateCodec.parse(value);
        return localDate == null ? null : toSqlDate(localDate);
    }

    private static LocalDate toLocalDate(final Date value) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(value.getTime()), ZoneId.systemDefault());
    }

    private static Date toSqlDate(final LocalDate localDate) {
        final long epochMillis = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return new Date(epochMillis);
    }
}
