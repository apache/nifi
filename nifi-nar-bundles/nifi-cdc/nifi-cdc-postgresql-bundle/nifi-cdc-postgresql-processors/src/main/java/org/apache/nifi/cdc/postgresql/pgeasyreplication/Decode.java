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

package org.apache.nifi.cdc.postgresql.pgeasyreplication;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For reference: https://www.postgresql.org/docs/10/protocol-logicalrep-message-formats.html
 */
public class Decode {

    private final Map<Integer, String> dataTypes = new HashMap<>();
    private final Map<Integer, Relation> relations = new HashMap<>();

    public Map<String, Object> decodeLogicalReplicationMessage(ByteBuffer buffer, boolean withBeginCommit) throws ParseException, SQLException, UnsupportedEncodingException {

        Map<String, Object> message = new HashMap<>();

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
        case 'B': /* Identifies the message as a begin message. */
            if (withBeginCommit) {

                message.put("type", "begin");

                message.put("xLSNFinal", buffer.getLong(1)); /* (Int64) The final LSN of the transaction. */
                message.put("xCommitTime",
                        getFormattedPostgreSQLEpochDate(buffer.getLong(9))); /*
                         * (Int64) Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
                         */
                message.put("xid", buffer.getInt(17)); /* (Int32) Xid of the transaction. */
            }

            return message;

        case 'C': /* Identifies the message as a commit message. */
            if (withBeginCommit) {

                message.put("type", "commit");

                message.put("flags", buffer.get(1)); /* (Int8) Flags; currently unused (must be 0). */
                message.put("commitLSN", buffer.getLong(2)); /* (Int64) The LSN of the commit. */
                message.put("xLSNEnd", buffer.getLong(10)); /* (Int64) The end LSN of the transaction. */
                message.put("xCommitTime", getFormattedPostgreSQLEpochDate(
                        buffer.getLong(18))); /*
                         * (Int64) Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
                         */
            }

            return message;

        case 'O': /* Identifies the message as an origin message. */

            message.put("type", "origin");

            message.put("originLSN", buffer.getLong(1)); /* (Int64) The LSN of the commit on the origin server. */

            buffer.position(9);
            byte[] bytes_O = new byte[buffer.remaining()];
            buffer.get(bytes_O);

            message.put("originName", new String(bytes_O, StandardCharsets.UTF_8)); /* (String) Name of the origin. */

            return message;

        case 'R': /* Identifies the message as a relation message. */

            message.put("type", "relation");

            message.put("relationId", buffer.getInt(position)); /* (Int32) ID of the relation. */
            position += 4;

            buffer.position(0);
            byte[] bytes_R = new byte[buffer.capacity()];
            buffer.get(bytes_R);
            String string_R = new String(bytes_R, StandardCharsets.UTF_8);

            int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
            int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

            message.put("namespaceName", string_R.substring(position, firstStringEnd)); /* (String) Namespace (empty string for pg_catalog). */
            message.put("relationName", string_R.substring(firstStringEnd + 1, secondStringEnd)); /* (String) Relation name. */

            /* next position = current position + string length + 1 */
            position += ((String) message.get("namespaceName")).length() + 1 + ((String) message.get("relationName")).length() + 1;

            buffer.position(position);

            message.put("relReplIdent", String.valueOf((char) buffer.get(position))); /*
             * (Int8) Replica identity setting for the relation (same as relreplident in pg_class).
             */
            position += 1;

            message.put("numColumns", buffer.getShort(position)); /* (Int16) Number of columns. */
            position += 2;

            List<Map<String, Object>> columns = new ArrayList<>();

            for (int i = 0; i < ((Short) message.get("numColumns")); i++) {

                Map<String, Object> column = new HashMap<>();

                column.put("isKey", buffer.get(position)); /*
                 * (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
                 */
                position += 1;

                column.put("columnName", string_R.substring(position, string_R.indexOf(0, position))); /* (String) Name of the column. */
                position += ((String) column.get("columnName")).length() + 1;

                column.put("dataTypeColId", buffer.getInt(position)); /* (Int32) ID of the column's data type. */
                position += 4;

                column.put("typeSpecificData", buffer.getInt(position)); /* (Int32) Type modifier of the column (atttypmod). */
                position += 4;

                columns.add(column);
            }

            message.put("columns", columns);

            return message;

        case 'Y': /* Identifies the message as a type message. */

            message.put("type", "type");

            message.put("dataTypeId", buffer.getInt(position)); /* (Int32) ID of the data type. */
            position += 4;

            buffer.position(0);
            byte[] bytes_Y = new byte[buffer.capacity()];
            buffer.get(bytes_Y);
            String string_Y = new String(bytes_Y, StandardCharsets.UTF_8);

            message.put("namespaceName", string_Y.substring(position, string_Y.indexOf(0, position))); /* (String) Namespace (empty string for pg_catalog). */
            position += ((String) message.get("namespaceName")).length() + 1;

            message.put("dataTypeName", string_Y.substring(position, string_Y.indexOf(0, position))); /* (String) Name of the data type. */

            return message;

        case 'I': /* Identifies the message as an insert message. */

            message.put("type", "insert");

            message.put("relationId", buffer.getInt(1)); /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            message.put("tupleType", String.valueOf((char) buffer.get(5))); /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */

            message.put("tupleData", parseTupleData(buffer, 6).getData()); /* (TupleData) TupleData message part representing the contents of new tuple. */

            return message;

        case 'U': /* Identifies the message as an update message. */

            message.put("type", "update");

            message.put("relationId", buffer.getInt(position));
            /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            position += 4;

            char tupleType1 = (char) buffer.get(position);
            message.put("tupleType1", String.valueOf(tupleType1));
            /*
             * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N').
             */
            position += 1;

            TupleData tupleData1 = parseTupleData(buffer, position); /* TupleData N, K or O */
            message.put("tupleData1", tupleData1.getData());
            position = tupleData1.getPosition();

            if (tupleType1 == 'N') {
                return message;
            }

            message.put("tupleType2", String.valueOf((char) buffer.get(position)));
            position += 1;

            TupleData tupleData2 = parseTupleData(buffer, position); /* TupleData N */
            message.put("tupleData2", tupleData2.getData());

            return message;

        case 'D': /* Identifies the message as a delete message. */

            message.put("type", "delete");

            message.put("relationId", buffer.getInt(position)); /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            position += 4;

            message.put("tupleType", String.valueOf((char) buffer.get(position))); /*
             * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O').
             */
            position += 1;

            message.put("tupleData", parseTupleData(buffer, position).getData()); /* TupleData */

            return message;

        default:

            message.put("type", "error");
            message.put("description", "Unknown message type \"" + msgType + "\".");
            return message;
        }
    }

    public TupleData parseTupleData(ByteBuffer buffer, int position) throws SQLException, UnsupportedEncodingException {

        Map<String, Object> data = new HashMap<>();

        StringBuilder values = new StringBuilder();

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /*
             * (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted value ('t').
             */
            position += 1; /* byte = 1 byte */

            if (i > 0) {
                values.append(',');
            }
            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length * bytes */

                values.append(new String(bytes, StandardCharsets.UTF_8)); /* (ByteN) The value of the column, in text format. */

            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */
                if (statusValue == 'n') {
                    values.append("null");
                } else {
                    values.append("UTOAST");
                }
            }
        }

        data.put("numColumns", columns);
        data.put("values", "(" + values + ")");

        return new TupleData(data, position);
    }

    public Map<String, Object> decodeLogicalReplicationMessageSimple(ByteBuffer buffer, boolean withBeginCommit) throws SQLException, UnsupportedEncodingException {

        Map<String, Object> message = new HashMap<>();

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
        case 'B': /* Identifies the message as a begin message. */

            if (withBeginCommit) {
                message.put("type", "begin");
            }

            return message;

        case 'C': /* Identifies the message as a commit message. */

            if (withBeginCommit) {
                message.put("type", "commit");
            }

            return message;

        case 'O': /* Identifies the message as an origin message. */

            message.put("type", "origin");
            return message;

        case 'R': /* Identifies the message as a relation message. */

            Relation relation = new Relation();

            relation.setId(buffer.getInt(position)); /* (Int32) ID of the relation. */
            position += 4;

            buffer.position(0);
            byte[] bytes_R = new byte[buffer.capacity()];
            buffer.get(bytes_R);
            String string_R = new String(bytes_R, StandardCharsets.UTF_8);

            int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
            int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

            relation.setNamespace(string_R.substring(position, firstStringEnd)); /* (String) Namespace (empty string for pg_catalog). */
            relation.setName(string_R.substring(firstStringEnd + 1, secondStringEnd)); /* (String) Relation name. */

            position += relation.getNamespace().length() + 1 + relation.getName().length() + 1; /* next position = current position + string length + 1 */

            buffer.position(position);

            relation.setReplicaIdentity((char) buffer.get(position)); /*
             * (Int8) Replica identity setting for the relation (same as relreplident in pg_class).
             */
            position += 1;

            relation.setNumColumns(buffer.getShort(position)); /* (Int16) Number of columns. */
            position += 2;

            for (int i = 0; i < relation.getNumColumns(); i++) {
                Column column = new Column();

                column.setIsKey((char) buffer.get(position)); /*
                 * (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
                 */
                position += 1;

                column.setName(string_R.substring(position, string_R.indexOf(0, position))); /* (String) Name of the column. */
                position += column.getName().length() + 1;

                column.setDataTypeId(buffer.getInt(position)); /* (Int32) ID of the column's data type. */
                position += 4;

                column.setDataTypeName(dataTypes.get(column.getDataTypeId()));

                column.setTypeModifier(buffer.getInt(position)); /* (Int32) Type modifier of the column (atttypmod). */
                position += 4;

                relation.putColumn(i, column);
            }

            relations.put(relation.getId(), relation);

            return message;

        case 'Y': /* Identifies the message as a type message. */

            message.put("type", "type");
            return message;

        case 'I': /* Identifies the message as an insert message. */

            message.put("type", "insert");

            int relationId_I = buffer.getInt(position); /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            position += 4;

            position += 1; /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */

            message.put("relationName", relations.get(relationId_I).getFullName());
            message.put("tupleData", parseTupleDataSimple(relationId_I, buffer, position).getData());

            return message;

        case 'U': /* Identifies the message as an update message. */

            message.put("type", "update");

            int relationId_U = buffer.getInt(position); /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            position += 4;

            char tupleType1 = (char) buffer.get(position); /*
             * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N').
             */
            position += 1;

            TupleData tupleData1 = parseTupleDataSimple(relationId_U, buffer, position); /* TupleData N, K or O */

            if (tupleType1 == 'N') {
                message.put("relationName", relations.get(relationId_U).getFullName());
                message.put("tupleData", tupleData1.getData());
                return message;
            }

            position = tupleData1.getPosition();

            position += 1; /*
             * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N').
             */

            message.put("relationName", this.relations.get(relationId_U).getFullName());
            message.put("tupleData", parseTupleDataSimple(relationId_U, buffer, position).getData()); /* TupleData N */

            return message;

        case 'D': /* Identifies the message as a delete message. */

            message.put("type", "delete");

            int relationId_D = buffer.getInt(position); /*
             * (Int32) ID of the relation corresponding to the ID in the relation message.
             */
            position += 4;

            position += 1; /*
             * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O').
             */

            message.put("relationName", this.relations.get(relationId_D).getFullName());
            message.put("tupleData", parseTupleDataSimple(relationId_D, buffer, position).getData()); /* TupleData */

            return message;

        default:

            message.put("type", "error");
            message.put("description", "Unknown message type \"" + msgType + "\".");
            return message;
        }
    }

    private TupleData parseTupleDataSimple(int relationId, ByteBuffer buffer, int position) throws SQLException, UnsupportedEncodingException {

        Map<String, Object> data = new HashMap<>();

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /*
             * (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted value ('t').
             */
            position += 1; /* byte = 1 byte */

            Column column = relations.get(relationId).getColumn(i);

            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length * bytes */

                if (column.getDataTypeName().startsWith("int")) { /*
                 * (ByteN) The value of the column, in text format. Numeric types are not quoted.
                 */
                    data.put(column.getName(), Long.valueOf(new String(bytes, StandardCharsets.UTF_8)));
                } else {
                    data.put(column.getName(), new String(bytes, StandardCharsets.UTF_8)); /* (ByteN) The value of the column, in text format. */
                }

            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */
                if (statusValue == 'n') {
                    data.put(column.getName(), null);
                } else {
                    data.put(column.getName(), "UTOAST");
                }
            }
        }

        return new TupleData(data, position);
    }

    private String getFormattedPostgreSQLEpochDate(long microseconds) throws ParseException {

        Date pgEpochDate = new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(pgEpochDate);
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + (int) (microseconds / 1000000));

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z Z").format(cal.getTime());
    }

    public void loadDataTypes(ConnectionManager connectionManager) throws SQLException {
        try (Statement stmt = connectionManager.getSQLConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("SELECT oid, typname FROM pg_catalog.pg_type")) {
            while (rs.next()) {
                dataTypes.put(rs.getInt(1), rs.getString(2));
            }
        }
    }

}
