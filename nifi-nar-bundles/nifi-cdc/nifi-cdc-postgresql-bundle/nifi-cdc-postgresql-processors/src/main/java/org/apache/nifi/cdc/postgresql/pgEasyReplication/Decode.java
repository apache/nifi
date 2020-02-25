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

package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.nifi.cdc.postgresql.pgEasyReplication.ConnectionManager;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Decode {
    private static HashMap<Integer, String> dataTypes = new HashMap<Integer, String>();
    private HashMap<Integer, Relation> relations = new HashMap<Integer, Relation>();

    @SuppressWarnings("unchecked")
    public JSONObject decodeLogicalReplicationMessage(ByteBuffer buffer, JSONObject json, boolean withBeginCommit) throws ParseException, SQLException, UnsupportedEncodingException {

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
        case 'B': /* Identifies the message as a begin message. */
            if (withBeginCommit) {

                JSONObject jsonMessage_B = new JSONObject();

                jsonMessage_B.put("xLSNFinal", buffer.getLong(1)); /* (Int64) The final LSN of the transaction. */
                jsonMessage_B.put("xCommitTime", getFormattedPostgreSQLEpochDate(
                        buffer.getLong(9))); /* (Int64) Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01). */
                jsonMessage_B.put("xid", buffer.getInt(17)); /* (Int32) Xid of the transaction. */

                json.put("begin", jsonMessage_B);
            }

            return json;

        case 'C': /* Identifies the message as a commit message. */
            if (withBeginCommit) {

                JSONObject jsonMessage_C = new JSONObject();

                jsonMessage_C.put("flags", buffer.get(1)); /* (Int8) Flags; currently unused (must be 0). */
                jsonMessage_C.put("commitLSN", buffer.getLong(2)); /* (Int64) The LSN of the commit. */
                jsonMessage_C.put("xLSNEnd", buffer.getLong(10)); /* (Int64) The end LSN of the transaction. */
                jsonMessage_C.put("xCommitTime", getFormattedPostgreSQLEpochDate(
                        buffer.getLong(18))); /* (Int64) Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01). */

                json.put("commit", jsonMessage_C);
            }

            return json;

        case 'O': /* Identifies the message as an origin message. */

            JSONObject jsonMessage_O = new JSONObject();

            jsonMessage_O.put("originLSN", buffer.getLong(1)); /* (Int64) The LSN of the commit on the origin server. */

            buffer.position(9);
            byte[] bytes_O = new byte[buffer.remaining()];
            buffer.get(bytes_O);

            jsonMessage_O.put("originName", new String(bytes_O, "UTF-8")); /* (String) Name of the origin. */

            json.put("origin", jsonMessage_O);
            return json;

        case 'R': /* Identifies the message as a relation message. */

            JSONObject jsonMessage_R = new JSONObject();

            jsonMessage_R.put("relationId", buffer.getInt(position)); /* (Int32) ID of the relation. */
            position += 4;

            buffer.position(0);
            byte[] bytes_R = new byte[buffer.capacity()];
            buffer.get(bytes_R);
            String string_R = new String(bytes_R, "UTF-8");

            int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
            int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

            jsonMessage_R.put("namespaceName", string_R.substring(position, firstStringEnd)); /* (String) Namespace (empty string for pg_catalog). */
            jsonMessage_R.put("relationName", string_R.substring(firstStringEnd + 1, secondStringEnd)); /* (String) Relation name. */

            /* next position = current position + string length + 1 */
            position += ((String) jsonMessage_R.get("namespaceName")).length() + 1 + ((String) jsonMessage_R.get("relationName")).length() + 1;

            buffer.position(position);

            jsonMessage_R.put("relReplIdent", "" + (char) buffer.get(position)); /* (Int8) Replica identity setting for the relation (same as relreplident in pg_class). */
            position += 1;

            jsonMessage_R.put("numColumns", buffer.getShort(position)); /* (Int16) Number of columns. */
            position += 2;

            JSONArray columns = new JSONArray();

            for (int i = 0; i < ((Short) jsonMessage_R.get("numColumns")); i++) {
                JSONObject column = new JSONObject();

                column.put("isKey", buffer.get(position)); /* (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key. */
                position += 1;

                column.put("columnName", string_R.substring(position, string_R.indexOf(0, position))); /* (String) Name of the column. */
                position += ((String) column.get("columnName")).length() + 1;

                column.put("dataTypeColId", buffer.getInt(position)); /* (Int32) ID of the column's data type. */
                position += 4;

                column.put("typeSpecificData", buffer.getInt(position)); /* (Int32) Type modifier of the column (atttypmod). */
                position += 4;

                columns.add(column);
            }

            jsonMessage_R.put("columns", columns);

            json.put("relation", jsonMessage_R);
            return json;

        case 'Y': /* Identifies the message as a type message. */

            JSONObject jsonMessage_Y = new JSONObject();

            jsonMessage_Y.put("dataTypeId", buffer.getInt(position)); /* (Int32) ID of the data type. */
            position += 4;

            buffer.position(0);
            byte[] bytes_Y = new byte[buffer.capacity()];
            buffer.get(bytes_Y);
            String string_Y = new String(bytes_Y, "UTF-8");

            jsonMessage_Y.put("namespaceName", string_Y.substring(position, string_Y.indexOf(0, position))); /* (String) Namespace (empty string for pg_catalog). */
            position += ((String) jsonMessage_Y.get("namespaceName")).length() + 1;

            jsonMessage_Y.put("dataTypeName", string_Y.substring(position, string_Y.indexOf(0, position))); /* (String) Name of the data type. */

            json.put("type", jsonMessage_Y);
            return json;

        case 'I': /* Identifies the message as an insert message. */

            JSONObject jsonMessage_I = new JSONObject();

            jsonMessage_I.put("relationId", buffer.getInt(1)); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            jsonMessage_I.put("tupleType", "" + (char) buffer.get(5)); /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */

            jsonMessage_I.put("tupleData", parseTupleData(buffer, 6)[0]); /* (TupleData) TupleData message part representing the contents of new tuple. */

            json.put("insert", jsonMessage_I);
            return json;

        case 'U': /* Identifies the message as an update message. */

            JSONObject jsonMessage_U = new JSONObject();

            jsonMessage_U.put("relationId", buffer.getInt(position)); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            position += 4;

            jsonMessage_U.put("tupleType1",
                    "" + (char) buffer.get(position)); /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N'). */
            position += 1;

            Object[] tupleData1 = parseTupleData(buffer, position); /* TupleData N, K or O */
            jsonMessage_U.put("tupleData1", tupleData1[0]);
            position = (Integer) tupleData1[1];

            if (jsonMessage_U.get("tupleType1") == "N") {
                json.put("update", jsonMessage_U);
                return json;
            }

            jsonMessage_U.put("tupleType2", "" + (char) buffer.get(position));
            position += 1;

            Object[] tupleData2 = parseTupleData(buffer, position); /* TupleData N */
            jsonMessage_U.put("tupleData2", tupleData2[0]);

            json.put("update", jsonMessage_U);
            return json;

        case 'D': /* Identifies the message as a delete message. */

            JSONObject jsonMessage_D = new JSONObject();

            jsonMessage_D.put("relationId", buffer.getInt(position)); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            position += 4;

            jsonMessage_D.put("tupleType", "" + (char) buffer.get(position)); /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O'). */
            position += 1;

            jsonMessage_D.put("tupleData", parseTupleData(buffer, position)[0]); /* TupleData */

            json.put("delete", jsonMessage_D);
            return json;

        default:

            json.put("error", "Unknown message type \"" + msgType + "\".");
            return json;
        }
    }

    @SuppressWarnings("unchecked")
    public Object[] parseTupleData(ByteBuffer buffer, int position) throws SQLException, UnsupportedEncodingException {

        JSONObject json = new JSONObject();
        Object[] result = { json, position };

        String values = "";

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /* (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted value ('t'). */
            position += 1; /* byte = 1 byte */

            if (i > 0)
                values += ",";

            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length * bytes */

                values += new String(bytes, "UTF-8"); /* (ByteN) The value of the column, in text format. */

            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */

                values = (statusValue == 'n') ? values + "null" : values + "UTOAST";
            }
        }

        json.put("numColumns", columns);
        json.put("values", "(" + values + ")");

        result[0] = json;
        result[1] = position;

        return result;
    }

    @SuppressWarnings("unchecked")
    public JSONObject decodeLogicalReplicationMessageSimple(ByteBuffer buffer, JSONObject json, boolean withBeginCommit) throws ParseException, SQLException, UnsupportedEncodingException {

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
        case 'B': /* Identifies the message as a begin message. */

            if (withBeginCommit)
                json.put("begin", "begin");

            return json;

        case 'C': /* Identifies the message as a commit message. */

            if (withBeginCommit)
                json.put("commit", "commit");

            return json;

        case 'O': /* Identifies the message as an origin message. */

            json.put("origin", "origin");
            return json;

        case 'R': /* Identifies the message as a relation message. */

            Relation relation = new Relation();

            relation.setId(buffer.getInt(position)); /* (Int32) ID of the relation. */
            position += 4;

            buffer.position(0);
            byte[] bytes_R = new byte[buffer.capacity()];
            buffer.get(bytes_R);
            String string_R = new String(bytes_R, "UTF-8");

            int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
            int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

            relation.setNamespace(string_R.substring(position, firstStringEnd)); /* (String) Namespace (empty string for pg_catalog). */
            relation.setName(string_R.substring(firstStringEnd + 1, secondStringEnd)); /* (String) Relation name. */

            position += relation.getNamespace().length() + 1 + relation.getName().length() + 1; /* next position = current position + string length + 1 */

            buffer.position(position);

            relation.setReplicaIdentity((char) buffer.get(position)); /* (Int8) Replica identity setting for the relation (same as relreplident in pg_class). */
            position += 1;

            relation.setNumColumns(buffer.getShort(position)); /* (Int16) Number of columns. */
            position += 2;

            for (int i = 0; i < relation.getNumColumns(); i++) {
                Column column = new Column();

                column.setIsKey((char) buffer.get(position)); /* (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key. */
                position += 1;

                column.setName(string_R.substring(position, string_R.indexOf(0, position))); /* (String) Name of the column. */
                position += column.getName().length() + 1;

                column.setDataTypeId(buffer.getInt(position)); /* (Int32) ID of the column's data type. */
                position += 4;

                column.setDataTypeName(Decode.dataTypes.get(column.getDataTypeId()));

                column.setTypeModifier(buffer.getInt(position)); /* (Int32) Type modifier of the column (atttypmod). */
                position += 4;

                relation.putColumn(i, column);
                ;
            }

            this.relations.put(relation.getId(), relation);

            return json;

        case 'Y': /* Identifies the message as a type message. */

            json.put("type", "type");
            return json;

        case 'I': /* Identifies the message as an insert message. */

            JSONObject jsonMessage_I = new JSONObject();

            int relationId_I = buffer.getInt(position); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            position += 4;

            position += 1; /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */

            jsonMessage_I.put(this.relations.get(relationId_I).getFullName(), parseTupleDataSimple(relationId_I, buffer, position)[0]);

            json.put("insert", jsonMessage_I);
            return json;

        case 'U': /* Identifies the message as an update message. */

            JSONObject jsonMessage_U = new JSONObject();

            int relationId_U = buffer.getInt(position); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            position += 4;

            char tupleType1 = (char) buffer.get(position); /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N'). */
            position += 1;

            Object[] tupleData1 = parseTupleDataSimple(relationId_U, buffer, position); /* TupleData N, K or O */

            if (tupleType1 == 'N') {
                jsonMessage_U.put(this.relations.get(relationId_U).getFullName(), tupleData1[0]);
                json.put("update", jsonMessage_U);
                return json;
            }

            position = (Integer) tupleData1[1];

            position += 1; /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N'). */

            jsonMessage_U.put(this.relations.get(relationId_U).getFullName(), parseTupleDataSimple(relationId_U, buffer, position)[0]); /* TupleData N */

            json.put("update", jsonMessage_U);
            return json;

        case 'D': /* Identifies the message as a delete message. */

            JSONObject jsonMessage_D = new JSONObject();

            int relationId_D = buffer.getInt(position); /* (Int32) ID of the relation corresponding to the ID in the relation message. */
            position += 4;

            position += 1; /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O'). */

            jsonMessage_D.put(this.relations.get(relationId_D).getFullName(), parseTupleDataSimple(relationId_D, buffer, position)[0]); /* TupleData */

            json.put("delete", jsonMessage_D);
            return json;

        default:

            json.put("error", "Unknown message type \"" + msgType + "\".");
            return json;
        }
    }

    @SuppressWarnings("unchecked")
    public Object[] parseTupleDataSimple(int relationId, ByteBuffer buffer, int position) throws SQLException, UnsupportedEncodingException {

        JSONObject json = new JSONObject();
        Object[] result = { json, position };

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /* (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted value ('t'). */
            position += 1; /* byte = 1 byte */

            Column column = relations.get(relationId).getColumn(i);

            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length * bytes */

                if (column.getDataTypeName().startsWith("int")) { /* (ByteN) The value of the column, in text format. Numeric types are not quoted. */
                    json.put(column.getName(), Long.parseLong(new String(bytes, "UTF-8")));
                } else {
                    json.put(column.getName(), new String(bytes, "UTF-8")); /* (ByteN) The value of the column, in text format. */
                }

            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */
                if (statusValue == 'n') {
                    json.put(column.getName(), null);
                } else {
                    json.put(column.getName(), "UTOAST");
                }
                ;
            }
        }

        result[0] = json;
        result[1] = position;

        return result;
    }

    public String getFormattedPostgreSQLEpochDate(long microseconds) throws ParseException {

        Date pgEpochDate = new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(pgEpochDate);
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + (int) (microseconds / 1000000));

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z Z").format(cal.getTime());
    }

    public void loadDataTypes(ConnectionManager connectionManager) throws SQLException {

        Statement stmt = connectionManager.getSQLConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = stmt.executeQuery("SELECT oid, typname FROM pg_catalog.pg_type");

        while (rs.next()) {
            Decode.dataTypes.put(rs.getInt(1), rs.getString(2));
        }

        rs.close();
        stmt.close();
    }

    public void printBuffer(ByteBuffer buffer) throws UnsupportedEncodingException {

        byte[] bytesX = new byte[buffer.capacity()];
        buffer.get(bytesX);

        String stringX = new String(bytesX, "UTF-8");

        char[] charsX = stringX.toCharArray();

        int icharX = 0;

        for (char c : charsX) {
            int ascii = c;
            System.out.println("[" + icharX + "] \t" + c + "\t ASCII " + ascii);
            icharX++;
        }
    }
}
