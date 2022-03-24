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
package org.apache.nifi.cef;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.SchemaInferenceUtil;

/**
 * Provides strategy for resolving data type for custom extensions.
 */
interface CEFCustomExtensionTypeResolver {

    /**
     * @param value String representation of the field value.
     *
     * @return The resolved data type matches the given value most, based on the implemented strategy.
     */
    DataType resolve(String value);

    CEFCustomExtensionTypeResolver STRING_RESOLVER = value -> RecordFieldType.STRING.getDataType();
    CEFCustomExtensionTypeResolver SIMPLE_RESOLVER = value -> SchemaInferenceUtil.getDataType(value);
    CEFCustomExtensionTypeResolver SKIPPING_RESOLVER = value -> null;
}
