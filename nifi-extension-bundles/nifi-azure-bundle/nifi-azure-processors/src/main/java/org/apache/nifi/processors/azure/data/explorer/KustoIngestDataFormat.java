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
package org.apache.nifi.processors.azure.data.explorer;

import org.apache.nifi.components.DescribedValue;

public enum KustoIngestDataFormat implements DescribedValue {
    AVRO("avro", "An Avro format with support for logical types and for the snappy compression codec"),
    APACHEAVRO("apacheavro", "An Avro format with support for logical types and for the snappy compression codec."),
    CSV("csv", "A text file with comma-separated values (,). For more information, see RFC 4180: Common Format " +
            "and MIME Type for Comma-Separated Values (CSV) Files."),
    JSON("json", "A text file containing JSON objects separated by \\n or \\r\\n. For more information, " +
            "see JSON Lines (JSONL)."),
    MULTIJSON("multijson", "A text file containing a JSON array of property containers (each representing a record) or any " +
            "number of property containers separated by spaces, \\n or \\r\\n. Each property container may be " +
            "spread across multiple lines. This format is preferable to JSON unless the data is not property " +
            "containers."),
    ORC("orc", "An ORC file."),
    PARQUET("parquet", "A parquet file."),
    PSV("psv", "A text file with values separated by vertical bars (|)."),
    SCSV("scsv", "A text file with values separated by semicolons (;)."),
    SOHSV("sohsv", "A text file with SOH-separated values. (SOH is the ASCII code point 1. " +
            "This format is used by Hive in HDInsight)."),
    TSV("tsv", "A text file with tab delimited values (\\t)."),
    TSVE("tsve", "A text file with tab-delimited values (\\t). A backslash (\\) is used as escape character."),
    TXT("txt", "A text file with lines separated by \\n. Empty lines are skipped");

    private final String kustoValue;
    private final String description;

    KustoIngestDataFormat(String kustoValue, String description) {
        this.kustoValue = kustoValue;
        this.description = description;
    }

    public String getKustoValue() {
        return kustoValue;
    }

    @Override
    public String getValue() {
        return this.getKustoValue();
    }

    @Override
    public String getDisplayName() {
        return kustoValue;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
