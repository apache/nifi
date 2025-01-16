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

package org.apache.nifi.processor.util.bin;

import org.apache.nifi.components.DescribedValue;

public enum InsertionLocation implements DescribedValue {
    LAST_IN_BIN("Last in Bin", "Insert the FlowFile at the end of the Bin that is terminated"),
    FIRST_IN_NEW_BIN("First in New Bin", "Insert the FlowFile at the beginning of a newly created Bin"),
    ISOLATED("Isolated", "Insert the FlowFile into a new Bin and terminate the Bin immediately with the FlowFile as the only content");

    private final String name;
    private final String description;

    InsertionLocation(final String name, final String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
