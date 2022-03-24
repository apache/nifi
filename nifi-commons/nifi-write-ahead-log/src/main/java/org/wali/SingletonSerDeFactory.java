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

package org.wali;

public class SingletonSerDeFactory<T> implements SerDeFactory<T> {
    private final SerDe<T> serde;

    public SingletonSerDeFactory(final SerDe<T> serde) {
        this.serde = serde;
    }

    @Override
    public SerDe<T> createSerDe(final String encodingName) {
        return serde;
    }

    @Override
    public Object getRecordIdentifier(final T record) {
        return serde.getRecordIdentifier(record);
    }

    @Override
    public UpdateType getUpdateType(final T record) {
        return serde.getUpdateType(record);
    }

    @Override
    public String getLocation(final T record) {
        return serde.getLocation(record);
    }
}
