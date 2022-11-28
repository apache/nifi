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
package org.apache.nifi.kafka.shared.transaction;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Standard Transaction Identifier Supplier with optional prefix
 */
public class TransactionIdSupplier implements Supplier<String> {
    private static final String EMPTY_PREFIX = "";

    private final String prefix;

    public TransactionIdSupplier(final String prefix) {
        this.prefix = prefix == null ? EMPTY_PREFIX : prefix;
    }

    /**
     * Get Transaction Identifier consisting of a random UUID with configured prefix string
     *
     * @return Transaction Identifier
     */
    @Override
    public String get() {
        return prefix + UUID.randomUUID();
    }
}
