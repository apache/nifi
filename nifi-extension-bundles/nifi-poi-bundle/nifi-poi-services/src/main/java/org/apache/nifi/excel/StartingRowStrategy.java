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
package org.apache.nifi.excel;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

public enum StartingRowStrategy implements DescribedValue {
    STANDARD("Use %s rows after the starting row.".formatted(StartingRowStrategy.NUM_ROWS_TO_DETERMINE_TYPES)),
    ALL("Use all the rows after the starting row.");

    static final int NUM_ROWS_TO_DETERMINE_TYPES = 10; // NOTE: This number is arbitrary.
    static final AllowableValue USE_STARTING_ROW = new AllowableValue("Use Starting Row", "Use Starting Row",
            "The configured first row of the Excel file is a header line that contains the names of the columns. The schema will be derived by using the "
                    + "column names in the header of the first sheet and dependent on the strategy chosen either the subsequent "
                    + NUM_ROWS_TO_DETERMINE_TYPES + " rows or all of the subsequent rows. However the configured header rows of subsequent sheets are skipped. "
                    + "NOTE: If there are duplicate column names then each subsequent duplicate column name is given a one up number. "
                    + "For example, column names \"Name\", \"Name\" will be changed to \"Name\", \"Name_1\".");

    private final String description;

    StartingRowStrategy(String description) {
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }
}
