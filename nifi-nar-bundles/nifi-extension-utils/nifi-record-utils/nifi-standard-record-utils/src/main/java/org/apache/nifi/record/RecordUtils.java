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
package org.apache.nifi.record;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class RecordUtils {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("record-character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to encode/decode/convert between 'string' and 'bytes' types.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();

    /**
     * This property supports the subset of character sets that Jackson's JsonEncoder supports, and is used by JSON
     * record readers/writers
     */
    public static final PropertyDescriptor JSON_CHARSET = new PropertyDescriptor.Builder()
            .name("record-json-character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to encode/decode/convert between 'string' and 'bytes' types.")
            .allowableValues("UTF8", "UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF8")
            .required(true)
            .build();
}
