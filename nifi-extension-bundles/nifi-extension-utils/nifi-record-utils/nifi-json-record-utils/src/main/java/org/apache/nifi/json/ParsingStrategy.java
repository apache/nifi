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
package org.apache.nifi.json;

import org.apache.nifi.components.DescribedValue;

public enum ParsingStrategy implements DescribedValue {
    STANDARD("Standard", "Parse JSON per the JSON specification"),
    LENIENT("Lenient", """
            Parse JSON leniently allowing Java comments (/**/ and //), Yaml comments (start of line begins with #)
            , leading plus sign in numbers (e.g. +123), leading zeros in numbers (e.g. 0001),
            "missing" decimal numbers to end with a decimal point (e.g. 123.),
            "missing value" in an array (i.e. sequence of two commas, without value in-between e.g. ["A",,"C"]),
            trailing comma in an array or member in an object, use of single quotes for quoting strings (i.e. use of an apostrophe)
            and use of unquoted field names.""");

    private final String displayName;
    private final String description;

    ParsingStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
