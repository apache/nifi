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

package org.apache.nifi.cdc.postgresql.event;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * Decode is the class responsible for convert a binary buffer event in a
 * key-value message. The binary buffer represents a event change (BEGIN,
 * COMMIT, INSERT, UPDATE, DELETE, etc.) in format that is defined by the
 * decoding output plugin (pgoutput). Decode also uses dataType field to enrich
 * RELATION messages including the name of column data type, and relations field
 * to enrich INSERT, UPDATE and DELETE messages including the relation name.
 *
 * @see org.apache.nifi.cdc.postgresql.event.Table
 * @see org.apache.nifi.cdc.postgresql.event.Column
 */
public class Decode {

    private static HashMap<Integer, String> dataTypes = new HashMap<Integer, String>();
    private static HashMap<Integer, Table> relations = new HashMap<Integer, Table>();

    /**
     * Decodes the binary buffer event read from replication stream and returns a
     * key-value message.
     *
     * @param buffer
     *                           Event change encoded.
     * @param includeBeginCommit
     *                           If TRUE, BEGIN and COMMIT events are returned.
     *                           If FALSE, message returned for these events is
     *                           empty.
     * @param includeAllMetadata
     *                           If TRUE, all received metadata is included in the
     *                           message and all events are returned.
     *                           If FALSE, additional metadata (relation id, tuple
     *                           type, etc.) are not included in the message. Also,
     *                           message returned for RELATION, ORIGIN and TYPE
     *                           events is empty.
     * @return HashMap
     * @throws ParseException
     *                                      if decodes fails to PostgreSQL
     *                                      Epoch Dates
     * @throws UnsupportedEncodingException
     *                                      if decodes fails to convert bytes
     *                                      to String
     */
    public static HashMap<String, Object> decodeLogicalReplicationBuffer(ByteBuffer buffer, boolean includeBeginCommit,
            boolean includeAllMetadata) throws ParseException, UnsupportedEncodingException {

        HashMap<String, Object> message = new HashMap<String, Object>();

        /* (Byte1) Identifies the message type. */
        char msgType = (char) buffer.get(0);
        int position = 1;

        switch (msgType) {
            /* Identifies the message as a BEGIN message. */
            case 'B':

                if (includeBeginCommit || includeAllMetadata) {
                    message.put("type", "begin");

                    /* (Int64) The final LSN of the transaction. */
                    message.put("xLSNFinal", buffer.getLong(1));

                    /*
                     * (Int64) Commit timestamp of the transaction. The value is in number of
                     * microseconds since PostgreSQL epoch (2000-01-01).
                     */
                    message.put("xCommitTime",
                            getFormattedPostgreSQLEpochDate(buffer.getLong(9)));

                    /* (Int32) Xid of the transaction. */
                    message.put("xid", buffer.getInt(17));
                }
                return message;

            /* Identifies the message as a COMMIT message. */
            case 'C':

                if (includeBeginCommit || includeAllMetadata) {
                    message.put("type", "commit");

                    /* (Byte1) Flags; currently unused (must be 0). */
                    message.put("flags", buffer.get(1));

                    /* (Int64) The LSN of the commit. */
                    message.put("commitLSN", buffer.getLong(2));

                    /* (Int64) The end LSN of the transaction. */
                    message.put("xLSNEnd", buffer.getLong(10));

                    /*
                     * (Int64) Commit timestamp of the transaction. The value is in number of
                     * microseconds since PostgreSQL epoch (2000-01-01).
                     */
                    message.put("xCommitTime", getFormattedPostgreSQLEpochDate(
                            buffer.getLong(18)));
                }
                return message;

            /* Identifies the message as an ORIGIN message. */
            case 'O':

                if (includeAllMetadata) {
                    message.put("type", "origin");

                    /* (Int64) The LSN of the commit on the origin server. */
                    message.put("originLSN", buffer.getLong(1));

                    buffer.position(9);
                    byte[] bytes_O = new byte[buffer.remaining()];
                    buffer.get(bytes_O);

                    /* (String) Name of the origin. */
                    message.put("originName", new String(bytes_O, "UTF-8"));
                }
                return message;

            /* Identifies the message as a RELATION message. */
            case 'R':

                Table relation = new Table();

                /* (Int32) ID of the relation. */
                relation.setId(buffer.getInt(position));
                position += 4;

                buffer.position(0);
                byte[] bytes_R = new byte[buffer.capacity()];
                buffer.get(bytes_R);
                String string_R = new String(bytes_R, "UTF-8");

                /* ASCII 0 = Null */
                int firstStringEnd = string_R.indexOf(0, position);

                /* ASCII 0 = Null */
                int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1);

                /* (String) Namespace (empty string for pg_catalog). */
                relation.setNamespace(string_R.substring(position, firstStringEnd));

                /* (String) Relation name. */
                relation.setName(string_R.substring(firstStringEnd + 1, secondStringEnd));

                /* next position = current position + string length + 1 */
                position += relation.getNamespace().length() + 1 + relation.getName().length() + 1;

                buffer.position(position);

                /*
                 * (Byte1) Replica identity setting for the relation (same as relreplident in
                 * pg_class).
                 */
                relation.setReplicaIdentity((char) buffer.get(position));
                position += 1;

                /* (Int16) Number of columns. */
                relation.setNumColumns(buffer.getShort(position));
                position += 2;

                for (int i = 1; i <= relation.getNumColumns(); i++) {
                    Column column = new Column();

                    /* Position of column in the table. Index-based 1. */
                    column.setPosition(i);

                    /*
                     * (Byte1) Flags for the column. Currently can be either 0 for no flags or 1
                     * which marks the column as part of the key.
                     */
                    column.setIsKey(buffer.get(position));
                    position += 1;

                    /* (String) Name of the column. */
                    column.setName(string_R.substring(position, string_R.indexOf(0, position)));
                    position += column.getName().length() + 1;

                    /* (Int32) ID of the column's data type. */
                    column.setDataTypeId(buffer.getInt(position));
                    position += 4;

                    column.setDataTypeName(Decode.dataTypes.get(column.getDataTypeId()));

                    /* (Int32) Type modifier of the column (atttypmod). */
                    column.setTypeModifier(buffer.getInt(position));
                    position += 4;

                    relation.putColumn(i, column);
                }

                Decode.relations.put(relation.getId(), relation);

                if (includeAllMetadata) {
                    message.put("type", "relation");
                    message.put("id", relation.getId());
                    message.put("name", relation.getName());
                    message.put("objectName", relation.getObjectName());
                    message.put("replicaIdentity", relation.getReplicaIdentity());
                    message.put("numColumns", relation.getNumColumns());
                    message.put("columns", relation.getColumns());
                }

                return message;

            /* Identifies the message as a TYPE message. */
            case 'Y':

                if (includeAllMetadata) {
                    message.put("type", "type");

                    /* (Int32) ID of the data type. */
                    message.put("dataTypeId", buffer.getInt(position));
                    position += 4;

                    buffer.position(0);
                    byte[] bytes_Y = new byte[buffer.capacity()];
                    buffer.get(bytes_Y);
                    String string_Y = new String(bytes_Y, "UTF-8");

                    /* (String) Namespace (empty string for pg_catalog). */
                    message.put("namespaceName", string_Y.substring(position, string_Y.indexOf(0, position)));
                    position += ((String) message.get("namespaceName")).length() + 1;

                    /* (String) Name of the data type. */
                    message.put("dataTypeName", string_Y.substring(position, string_Y.indexOf(0, position)));
                }
                return message;

            /* Identifies the message as an INSERT message. */
            case 'I':

                message.put("type", "insert");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_I = buffer.getInt(position);
                position += 4;

                if (includeAllMetadata)
                    message.put("relationId", relationId_I);

                message.put("relationName", Decode.relations.get(relationId_I).getObjectName());

                if (includeAllMetadata)
                    /*
                     * (Byte1) Identifies the following TupleData message as a new tuple ('N').
                     */
                    message.put("tupleType", "" + (char) buffer.get(5));

                position += 1;

                message.put("tupleData", parseTupleData(relationId_I, buffer, position)[0]);

                return message;

            /* Identifies the message as an UPDATE message. */
            case 'U':

                message.put("type", "update");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_U = buffer.getInt(position);
                position += 4;

                if (includeAllMetadata)
                    message.put("relationId", relationId_U);

                message.put("relationName", Decode.relations.get(relationId_U).getObjectName());

                /*
                 * (Byte1) Either identifies the following TupleData submessage as a key ('K')
                 * or as an old tuple ('O') or as a new tuple ('N').
                 */
                char tupleType1 = (char) buffer.get(position);
                position += 1;

                if (includeAllMetadata)
                    message.put("tupleType1", tupleType1);

                /* TupleData N, K or O */
                Object[] tupleData1 = parseTupleData(relationId_U, buffer, position);

                if (includeAllMetadata)
                    message.put("tupleData1", tupleData1[0]);

                if (tupleType1 == 'N') {
                    if (!includeAllMetadata)
                        /* TupleData N */
                        message.put("tupleData", tupleData1[0]);

                    return message;
                }

                position = (Integer) tupleData1[1];

                if (includeAllMetadata) {
                    char tupleType2 = (char) buffer.get(position);

                    /*
                     * Byte1) Either identifies the following TupleData submessage as a key ('K') or
                     * as an old tuple ('O') or as a new tuple ('N').
                     */
                    message.put("tupleType2", tupleType2);
                }

                position += 1;

                if (includeAllMetadata)
                    /* TupleData N */
                    message.put("tupleData2", parseTupleData(relationId_U, buffer, position)[0]);
                else
                    /* TupleData N */
                    message.put("tupleData", parseTupleData(relationId_U, buffer, position)[0]);

                return message;

            /* Identifies the message as a delete message. */
            case 'D':

                message.put("type", "delete");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_D = buffer.getInt(position);
                position += 4;

                if (includeAllMetadata)
                    message.put("relationId", relationId_D);

                message.put("relationName", Decode.relations.get(relationId_D).getObjectName());

                if (includeAllMetadata)
                    /*
                     * (Byte1) Either identifies the following TupleData submessage as a key ('K')
                     * or as an old tuple ('O').
                     */
                    message.put("tupleType", "" + (char) buffer.get(position));

                position += 1;

                /* TupleData */
                message.put("tupleData", parseTupleData(relationId_D, buffer, position)[0]);

                return message;

            default:

                message.put("type", "error");
                message.put("description", "Unknown message type \"" + msgType + "\".");
                return message;
        }
    }

    /**
     * Decodes the tuple data (row) included in binary buffer for INSERT,
     * UPDATE and DELETE events.
     *
     * @param relationId
     *                   Table ID used to get the table name from Decode relations
     *                   field.
     * @param buffer
     *                   Binary buffer of the event.
     * @param position
     *                   Position in which to start tuple data.
     * @return Object[]
     * @throws UnsupportedEncodingException
     *                                      if decodes fails to convert bytes
     *                                      to String
     */
    public static Object[] parseTupleData(int relationId, ByteBuffer buffer, int position)
            throws UnsupportedEncodingException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        Object[] result = { data, position };

        /* (Int16) Number of columns. */
        short columns = buffer.getShort(position);
        /* short = 2 bytes */
        position += 2;

        for (int i = 1; i <= columns; i++) {
            /*
             * (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed
             * value ('u') or text formatted value ('t').
             */
            char dataFormat = (char) buffer.get(position);
            /* byte = 1 byte */
            position += 1;

            Column column = relations.get(relationId).getColumn(i);

            if (dataFormat == 't') {
                /* (Int32) Length of the column value. */
                int lenValue = buffer.getInt(position);
                /* int = 4 bytes */
                position += 4;

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                /* String = length * bytes */
                position += lenValue;

                /*
                 * (ByteN) The value of the column, in text format.
                 * Numeric types are not quoted.
                 */
                if (column.getDataTypeName().startsWith("int")) {
                    data.put(column.getName(), Long.parseLong(new String(bytes, "UTF-8")));
                } else {
                    /* (ByteN) The value of the column, in text format. */
                    data.put(column.getName(), new String(bytes, "UTF-8"));
                }

            } else { /* dataFormat = 'n' (NULL value) or 'u' (unchanged TOASTED value) */
                if (dataFormat == 'n') {
                    data.put(column.getName(), null);
                } else {
                    data.put(column.getName(), "UTOAST");
                }
            }
        }

        result[0] = data;
        result[1] = position;

        return result;
    }

    /**
     * Convert PostgreSQL epoch to human-readable date format.
     *
     * @param microseconds
     *                     Microseconds since 2000-01-01 00:00:00.000.
     * @return String
     * @throws ParseException
     *                        if fails to parse start date
     */
    public static String getFormattedPostgreSQLEpochDate(long microseconds) throws ParseException {

        Date pgEpochDate = new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(pgEpochDate);
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + (int) (microseconds / 1000000));

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z Z").format(cal.getTime());
    }

    /**
     * Indicates whether the dataTypes field is empty.
     *
     * @return boolean
     */
    public static boolean isDataTypesEmpty() {
        if (Decode.dataTypes.isEmpty())
            return true;

        return false;
    }

    /**
     * Loads the PostgreSQL data types set from pg_catalog.pg_type system view.
     * These set are used to enrich messages with the data type name.
     *
     * @param queryConnection
     *                        Connection to PostgreSQL database.
     * @throws SQLException
     *                      if fails to access PostgreSQL database
     */
    public static void loadDataTypes(Connection queryConnection) throws SQLException {
        Statement stmt = queryConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT oid, typname FROM pg_catalog.pg_type");

        while (rs.next()) {
            Decode.dataTypes.put(rs.getInt(1), rs.getString(2));
        }

        rs.close();
        stmt.close();
    }

    /**
     * Sets manually the data types set used to enrich messages.
     *
     * @param dataTypes
     *                  A set with data type id and data type name.
     */
    public static void setDataTypes(HashMap<Integer, String> dataTypes) {
        Decode.dataTypes = dataTypes;
    }
}
