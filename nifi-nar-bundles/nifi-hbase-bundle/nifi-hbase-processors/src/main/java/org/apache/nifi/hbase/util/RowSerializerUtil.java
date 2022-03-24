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
package org.apache.nifi.hbase.util;

import org.apache.nifi.hbase.scan.ResultCell;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class RowSerializerUtil {

    /**
     * @param rowId the row id to get the string from
     * @param charset the charset that was used to encode the cell's row
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's row
     */
    public static String getRowId(final byte[] rowId, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellRowBuffer = ByteBuffer.wrap(rowId);
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellRowBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(rowId, charset);
        }
    }

    /**
     * @param cell the cell to get the family from
     * @param charset the charset that was used to encode the cell's family
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's family
     */
    public static String getCellFamily(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellFamilyBuffer = ByteBuffer.wrap(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellFamilyBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), charset);
        }
    }

    /**
     * @param cell the cell to get the qualifier from
     * @param charset the charset that was used to encode the cell's qualifier
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's qualifier
     */
    public static String getCellQualifier(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellQualifierBuffer = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellQualifierBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), charset);
        }
    }

    /**
     * @param cell the cell to get the value from
     * @param charset the charset that was used to encode the cell's value
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's value
     */
    public static String getCellValue(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellValueBuffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellValueBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), charset);
        }
    }

}
