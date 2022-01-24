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

/**
 * Column is the class for a PostgreSQL Table Column metadata.
 * The Column object encapsulates the name, data type, position, if is part of
 * key and atttypmod attribute about a specific PostgreSQL Table Column.
 */
public class Column {

    private int position;
    private boolean isKey;
    private String name;
    private int dataTypeId;
    private String dataTypeName;
    private int typeModifier;

    /**
     * Returns the Column Position in the Table. Index-based 1.
     *
     * @return int Postion
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the Column Position in the Table.
     *
     * @param position
     *                 Index-based 1.
     */
    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * Indicates whether the Column is part of the Table Key.
     *
     * @return boolean
     */
    public boolean getIsKey() {
        return isKey;
    }

    /**
     * Sets 1 if the Column as part of the Table Key.
     *
     * @param isKey
     *              Byte 1 is true and 0 is false.
     */
    public void setIsKey(byte isKey) {
        this.isKey = isKey == 1 ? true : false;
    }

    /**
     * Returns the Column Name.
     *
     * @return String
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the Column Name.
     *
     * @param name
     *             Column Name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the ID of the Column Data Type. The OID column from
     * pg_catalog.pg_type.
     *
     * @return int
     */
    public int getDataTypeId() {
        return dataTypeId;
    }

    /**
     * Sets the ID of the Column Data Type.
     *
     * @param dataTypeId
     *                   The OID column from pg_catalog.pg_type.
     *                   For example, 23 is INT4 and 1043 is VARCHAR.
     */
    public void setDataTypeId(int dataTypeId) {
        this.dataTypeId = dataTypeId;
    }

    /**
     * Returns the Name of Column Data Type. The typename column from
     * pg_catalog.pg_type.
     *
     * @return String
     */
    public String getDataTypeName() {
        return dataTypeName;
    }

    /**
     * Sets the Name of Column Data Type.
     *
     * @param dataTypeName
     *                     The typename column from pg_catalog.pg_type.
     */
    public void setDataTypeName(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    /**
     * Returns the Column Attribute atttypmod. Records type-specific data supplied
     * at table creation time. The value will generally be -1 for types that do not
     * need atttypmod.
     *
     * @return int
     */
    public int getTypeModifier() {
        return typeModifier;
    }

    /**
     * Sets the Column Attribute atttypmod.
     *
     * @param typeModifier
     *                     The value will generally be -1 for types that do not need
     *                     atttypmod.
     */
    public void setTypeModifier(int typeModifier) {
        this.typeModifier = typeModifier;
    }
}
