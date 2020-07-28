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
package org.apache.nifi.toolkit.cli.impl.result.writer;

import org.apache.commons.lang3.Validate;

public class TableColumn {

    private final String name;
    private final int minLength;
    private final int maxLength;
    private final boolean abbreviate;

    public TableColumn(final String name, final int minLength, final int maxLength) {
        this(name, minLength, maxLength, false);
    }

    public TableColumn(final String name, final int minLength, final int maxLength, final boolean abbreviate) {
        this.name = name;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.abbreviate = abbreviate;
        Validate.notBlank(this.name);
        Validate.isTrue(this.minLength > 0);
        Validate.isTrue(this.maxLength > 0);
        Validate.isTrue(this.maxLength >= this.minLength);
    }

    public String getName() {
        return name;
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public boolean isAbbreviated() {
        return abbreviate;
    }

}
