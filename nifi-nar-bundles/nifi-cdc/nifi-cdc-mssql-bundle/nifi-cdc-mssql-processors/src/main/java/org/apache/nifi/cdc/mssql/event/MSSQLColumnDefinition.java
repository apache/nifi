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
package org.apache.nifi.cdc.mssql.event;

import org.apache.nifi.cdc.event.ColumnDefinition;

import java.sql.JDBCType;

public class MSSQLColumnDefinition extends ColumnDefinition {

    private boolean isKey=false;
    private int columnOrdinal;
    private int columnSize=255;

    public boolean getIsKey(){
        return isKey;
    }
    public int getColumnOrdinal(){
        return columnOrdinal;
    }

    public int getColumnSize(){
        return columnSize;
    }

    public void setColumnSize(int size){
        columnSize = size;
    }

    public String getCustomTypeName(){
        return "";
    }

    public boolean isRequired(){
        return isKey;
    }

    public void setIsKey(boolean isKey){
        this.isKey = isKey;
    }

    public JDBCType getDataType(){
        return JDBCType.valueOf(this.getType());

    }

    public MSSQLColumnDefinition(int type, String name, int columnOrdinal, boolean isKey) {
        super(type, name);

        this.isKey = isKey;
        this.columnOrdinal = columnOrdinal;
    }
}
