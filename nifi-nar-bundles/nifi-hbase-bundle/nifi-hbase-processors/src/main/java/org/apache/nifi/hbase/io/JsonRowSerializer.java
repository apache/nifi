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
package org.apache.nifi.hbase.io;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.hbase.scan.ResultCell;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Serializes a row from HBase to a JSON document of the form:
 *
 * {
 *  "row" : "row1",
 *  "cells": {
 *      "fam1:qual1" : "val1",
 *      "fam1:qual2" : "val2"
 *  }
 * }
 *
 */
public class JsonRowSerializer implements RowSerializer {

    private final Charset charset;

    public JsonRowSerializer(final Charset charset) {
        this.charset = charset;
    }

    @Override
    public String serialize(byte[] rowKey, ResultCell[] cells) {
        final StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        final String row = new String(rowKey, charset);
        jsonBuilder.append("\"row\":")
                .append("\"")
                .append(StringEscapeUtils.escapeJson(row))
                .append("\"");

        jsonBuilder.append(", \"cells\": {");
        int i = 0;
        for (final ResultCell cell : cells) {
            final String cellFamily = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), charset);
            final String cellQualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), charset);

            if (i > 0) {
                jsonBuilder.append(", ");
            }
            jsonBuilder.append("\"")
                    .append(StringEscapeUtils.escapeJson(cellFamily))
                    .append(":")
                    .append(StringEscapeUtils.escapeJson(cellQualifier))
                    .append("\":\"")
                    .append(StringEscapeUtils.escapeJson(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), charset)))
                    .append("\"");
            i++;
        }

        jsonBuilder.append("}}");
        return jsonBuilder.toString();
    }

    @Override
    public void serialize(final byte[] rowKey, final ResultCell[] cells, final OutputStream out) throws IOException {
        final String json = serialize(rowKey, cells);
        out.write(json.getBytes(charset));
    }

}
