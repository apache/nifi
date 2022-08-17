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
package org.apache.nifi.serialization.record.field;

import java.util.Optional;

/**
 * Generalized Field Converter interface for handling type conversion with optional format parsing
 *
 * @param <I> Input Field Type
 * @param <O> Output Field Type
 */
public interface FieldConverter<I, O> {
    /**
     * Convert Field using Output Field Type with optional format parsing
     *
     * @param field Input field to be converted
     * @param pattern Format pattern optional for parsing
     * @param name Input field name for tracking
     * @return Converted Field can be null when input field is null or empty
     */
    O convertField(I field, Optional<String> pattern, String name);
}
