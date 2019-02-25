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
package org.apache.nifi.util.hive;

public class CsvOutputOptions {

    private boolean header = true;
    private String altHeader = null;
    private String delimiter = ",";
    private boolean quote = false;
    private boolean escape = true;

    private int maxRowsPerFlowFile = 0;

    public boolean isHeader() {
        return header;
    }

    public String getAltHeader() {
        return altHeader;
    }


    public String getDelimiter() {
        return delimiter;
    }


    public boolean isQuote() {
        return quote;
    }

    public boolean isEscape() {
        return escape;
    }

    public int getMaxRowsPerFlowFile() {
        return maxRowsPerFlowFile;
    }

    public CsvOutputOptions(boolean header, String altHeader, String delimiter, boolean quote, boolean escape, int maxRowsPerFlowFile) {
        this.header = header;
        this.altHeader = altHeader;
        this.delimiter = delimiter;
        this.quote = quote;
        this.escape = escape;
        this.maxRowsPerFlowFile = maxRowsPerFlowFile;
    }
}
