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

import org.apache.nifi.cdc.mssql.MSSQLCDCUtils;
import org.apache.nifi.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TableCapturePlan {

    public enum PlanTypes{
        CDC,
        SNAPSHOT
    }

    public MSSQLTableInfo getTable() {
        return table;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public boolean getCaptureBaseline() {
        return captureBaseline;
    }

    public Timestamp getMaxTime() {
        return maxTime;
    }

    public PlanTypes getPlanType() {
        return planType;
    }

    private MSSQLTableInfo table;
    private int rowLimit;
    private boolean captureBaseline;
    private Timestamp maxTime;

    private PlanTypes planType;

    public TableCapturePlan(MSSQLTableInfo table, int rowLimit, boolean captureBaseline, String sTime){
        this.table = table;

        this.rowLimit = rowLimit;
        this.captureBaseline = captureBaseline;

        if(!StringUtils.isEmpty(sTime)){
            this.maxTime = Timestamp.valueOf(sTime);
        }
    }

    public void ComputeCapturePlan(Connection con, MSSQLCDCUtils mssqlcdcUtils) throws SQLException {
        //No Row Limit Options
        if(getRowLimit() == 0){
            //No starting point for CDC data extract
            if(getMaxTime() == null){
                //If we need a data baseline then capture a snapshot, otherwise CDC
                this.planType = getCaptureBaseline()?PlanTypes.SNAPSHOT:PlanTypes.CDC;
            }  else {
                //We have a starting point for data extraction
                this.planType = PlanTypes.CDC;
            }
        } else {
            //We may need to capture a data baseline depending on CDC Row Count

            //No starting point for CDC data extract
           if(getMaxTime() == null) {
                //There is no previous starting point, and we need a data baseline anyways
                // do a full snapshot
                if(getCaptureBaseline()){
                    this.planType = PlanTypes.SNAPSHOT;
                } else {
                    //There is no previous starting point, and depending on row count
                    // we may choose to pull a full snapshot anyways, even though we
                    // didn't ask to capture a baseline
                    String sqlQuery = mssqlcdcUtils.getCDCRowCountStatement(getTable(), null);
                    long rowCount = getRowCount(con, sqlQuery);

                    //If we didn't pass the Row Limit threshold, then use CDC, else get a Snapshot
                    this.planType = rowCount < getRowLimit()?PlanTypes.CDC:PlanTypes.SNAPSHOT;
                }
            } else {
               //We have a starting point for data extraction

               //Assume CDC unless we have a reason to change it
                this.planType = PlanTypes.CDC;

                String sqlQuery = mssqlcdcUtils.getCDCRowCountStatement(getTable(), getMaxTime());
                long rowCount = getRowCount(con, sqlQuery);

                if(rowCount >= getRowLimit()){
                    this.planType = PlanTypes.SNAPSHOT;
                }
            }
        }

        return;
    }

    private long getRowCount(Connection con, String sqlQuery) throws SQLException {
        try(final PreparedStatement st = con.prepareStatement(sqlQuery)) {
            if(getMaxTime() != null){
                st.setTimestamp(1, getMaxTime());
            }

            final ResultSet resultSet = st.executeQuery();
            resultSet.next();
            long rowCount = resultSet.getLong(1);

            resultSet.close();

            return rowCount;
        }
    }
}
