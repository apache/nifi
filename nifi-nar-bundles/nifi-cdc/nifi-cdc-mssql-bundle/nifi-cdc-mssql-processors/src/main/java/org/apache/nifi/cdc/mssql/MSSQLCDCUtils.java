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
package org.apache.nifi.cdc.mssql;

import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.mssql.event.MSSQLColumnDefinition;
import org.apache.nifi.cdc.mssql.event.MSSQLTableInfo;

import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MSSQLCDCUtils {
    private static final String _columnSplit = "\n,";

    final String LIST_CHANGE_TRACKING_TABLES_SQL = "SELECT object_id,\n" +
            "  DB_NAME() AS [databaseName], \n" +
            "  SCHEMA_NAME(OBJECTPROPERTY(object_id, 'SchemaId')) AS [schemaName], \n" +
            "  OBJECT_NAME(object_id) AS [tableName], \n" +
            "  SCHEMA_NAME(OBJECTPROPERTY(source_object_id, 'SchemaId')) AS [sourceSchemaName],\n" +
            "  OBJECT_NAME(source_object_id) AS [sourceTableName] \n" +
            "FROM [cdc].[change_tables]";

    final String LIST_TABLE_COLUMNS = "select cc.object_id\n" +
            ",cc.column_name\n" +
            ",cc.column_id\n" +
            ",cc.column_type\n" +
            ",cc.column_ordinal\n" +
            ",CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END \"key\"\n" +
            "FROM cdc.captured_columns cc\n" +
            "LEFT OUTER JOIN cdc.index_columns ic ON \n" +
            "(ic.object_id = cc.object_id AND ic.column_name = cc.column_name)\n" +
            "where cc.object_id=?\n" +
            "ORDER BY cc.column_ordinal";

    public String getLIST_CHANGE_TRACKING_TABLES_SQL(){
        return LIST_CHANGE_TRACKING_TABLES_SQL;
    }

    public String getLIST_TABLE_COLUMNS(){
        return LIST_TABLE_COLUMNS;
    }

    public String getCURRENT_TIMESTAMP(){
        return "CURRENT_TIMESTAMP";
    }

    public List<MSSQLTableInfo> getCDCTableList(Connection con) throws SQLException, CDCException {
        ArrayList<MSSQLTableInfo> cdcTables = new ArrayList<>();

        try(final Statement st = con.createStatement()){
            final ResultSet resultSet = st.executeQuery(getLIST_CHANGE_TRACKING_TABLES_SQL());

            while (resultSet.next()) {
                int objectId = resultSet.getInt("object_id");
                String databaseName = resultSet.getString("databaseName");
                String schemaName = resultSet.getString("schemaName");
                String tableName = resultSet.getString("tableName");
                String sourceSchemaName = resultSet.getString("sourceSchemaName");
                String sourceTableName = resultSet.getString("sourceTableName");

                MSSQLTableInfo ti = new MSSQLTableInfo(databaseName, schemaName, tableName, sourceSchemaName, sourceTableName, Integer.toUnsignedLong(objectId), null);
                cdcTables.add(ti);
            }

            for (MSSQLTableInfo ti:cdcTables) {
                List<ColumnDefinition> tableColums = getCaptureColumns(con, ti.getTableId());

                ti.setColumns(tableColums);
            }
        }

        return cdcTables;
    }

    public List<ColumnDefinition> getCaptureColumns(Connection con, long objectId) throws SQLException, CDCException {
        ArrayList<ColumnDefinition> tableColumns = new ArrayList<>();
        try(final PreparedStatement st = con.prepareStatement(getLIST_TABLE_COLUMNS())){
            st.setLong(1, objectId);

            final ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                String columnName = resultSet.getString("column_name");
                int columnId = resultSet.getInt("column_id");
                String columnType = resultSet.getString("column_type");
                int columnOrdinal = resultSet.getInt("column_ordinal");
                int isColumnKey = resultSet.getInt("key");

                int jdbcType = TranslateMSSQLTypeToJDBCTypes(columnType);

                //get column list
                MSSQLColumnDefinition col = new MSSQLColumnDefinition(jdbcType, columnName, columnOrdinal, isColumnKey==1);
                tableColumns.add(col);
            }
        } catch (SQLException e) {
            throw e;
        }

        return tableColumns;
    }

    public String getSnapshotSelectStatement(MSSQLTableInfo tableInfo){
        final StringBuilder sbQuery = new StringBuilder();

        sbQuery.append("SELECT " + getCURRENT_TIMESTAMP() + " tran_begin_time");

        sbQuery.append(_columnSplit);
        sbQuery.append(getCURRENT_TIMESTAMP() + " \"tran_end_time\"");

        sbQuery.append(_columnSplit);
        sbQuery.append("0 trans_id");

        sbQuery.append(_columnSplit);
        sbQuery.append("0 start_lsn");

        sbQuery.append(_columnSplit);
        sbQuery.append("0 seqval");

        sbQuery.append(_columnSplit);
        sbQuery.append("2 operation");

        sbQuery.append(_columnSplit);
        sbQuery.append("0 update_mask");

        for(ColumnDefinition col : tableInfo.getColumns()){
            MSSQLColumnDefinition mssqlColumnDefinition = (MSSQLColumnDefinition)col;

            sbQuery.append(_columnSplit);
            sbQuery.append("\"" + mssqlColumnDefinition.getName() + "\"");
        }

        sbQuery.append(_columnSplit);
        sbQuery.append(getCURRENT_TIMESTAMP() + " EXTRACT_TIME");
        sbQuery.append("\n");
        sbQuery.append("FROM " + tableInfo.getSourceSchemaName() + ".\""+ tableInfo.getSourceTableName() + "\"");

        return sbQuery.toString();
    }

    public String getCDCSelectStatement(MSSQLTableInfo tableInfo, Timestamp maxTime){
        final StringBuilder sbQuery = new StringBuilder();

        sbQuery.append("SELECT t.tran_begin_time\n" +
                ",t.tran_end_time \"tran_end_time\"\n" +
                ",CAST(t.tran_id AS bigint) trans_id\n" +
                ",CAST(\"o\".\"__$start_lsn\" AS bigint) start_lsn\n" +
                ",CAST(\"o\".\"__$seqval\" AS bigint) seqval\n" +
                ",\"o\".\"__$operation\" operation\n" +
                ",CAST(\"o\".\"__$update_mask\" AS bigint) update_mask");

        for(ColumnDefinition col : tableInfo.getColumns()){
            MSSQLColumnDefinition mssqlColumnDefinition = (MSSQLColumnDefinition)col;

            sbQuery.append(_columnSplit);
            sbQuery.append("\"o\".\"" + mssqlColumnDefinition.getName() + "\"");
        }

        sbQuery.append(_columnSplit);
        sbQuery.append(getCURRENT_TIMESTAMP() + " EXTRACT_TIME");
        sbQuery.append("\n");
        sbQuery.append("FROM cdc.\""+ tableInfo.getTableName() + "\" \"o\"\nINNER JOIN cdc.lsn_time_mapping t ON (t.start_lsn = \"o\".\"__$start_lsn\")");

        if(maxTime != null){
            sbQuery.append("\nWHERE t.tran_end_time > ?");
        }

        sbQuery.append("\nORDER BY CAST(\"o\".\"__$start_lsn\" AS bigint), \"o\".\"__$seqval\", \"o\".\"__$operation\"");

        return sbQuery.toString();
    }

    public String getCDCRowCountStatement(MSSQLTableInfo tableInfo, Timestamp maxTime){
        final StringBuilder sbQuery = new StringBuilder();

        sbQuery.append("SELECT COUNT(*) rowcnt \n");
        sbQuery.append("FROM cdc.\""+ tableInfo.getTableName() + "\" \"o\"\nINNER JOIN cdc.lsn_time_mapping t ON (t.start_lsn = \"o\".\"__$start_lsn\")");

        if(maxTime != null){
            sbQuery.append("\nWHERE t.tran_end_time > ?");
        }

        return sbQuery.toString();
    }

    //List from https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types
    public static int TranslateMSSQLTypeToJDBCTypes(String mssqltype) throws CDCException {
        switch (mssqltype){
            case "bigint":
                return Types.BIGINT;
            case "binary":
                return Types.BINARY;
            case "bit":
                return Types.BIT;
            case "char":
            case "uniqueidentifier":
                return Types.CHAR;
            case "date":
                return Types.DATE;
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                return Types.TIMESTAMP;
            case "decimal":
                return Types.DECIMAL;
            case "float":
                return Types.DOUBLE;
            case "image":
                return Types.LONGVARBINARY;
            case "int":
                return Types.INTEGER;
            case "money":
            case "smallmoney":
                return Types.DECIMAL;
            case "nchar":
                return Types.NCHAR;
            case "ntext":
            case "text":
            case "xml":
                return Types.LONGVARCHAR;
            case "numeric":
                return Types.NUMERIC;
            case "nvarchar":
            case "varchar":
                return Types.VARCHAR;
            case "real":
                return Types.REAL;
            case "smallint":
                return Types.SMALLINT;
            case "time":
                return Types.TIME;
            case "timestamp":
                return Types.BINARY;
            case "tinyint":
                return Types.TINYINT;
            case "udt":
            case "varbinary":
                return Types.VARBINARY;
        }

        throw new CDCException("Unrecognized MS SQL data type " + mssqltype);
    }
}

