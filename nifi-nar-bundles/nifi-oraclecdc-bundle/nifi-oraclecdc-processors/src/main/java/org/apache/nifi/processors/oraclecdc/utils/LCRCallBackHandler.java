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
package org.apache.nifi.processors.oraclecdc.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.codec.binary.Base32;
import org.apache.nifi.processor.exception.ProcessException;
import org.nifi.oraclecdcservice.api.OracleCDCEventHandler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class LCRCallBackHandler implements InvocationHandler {

    byte[] finalPosition;
    boolean moveWatermark = false;
    private ClassLoader classLoader;
    private ProcessLCR processLCR;
    private OracleCDCEventHandler handler;

    public LCRCallBackHandler(ClassLoader classLoader, OracleCDCEventHandler handler) {
        // TODO Auto-generated constructor stub
        this.classLoader = classLoader;
        this.processLCR = new ProcessLCR(classLoader);
        this.handler = handler;
    }

    public Object createChunk() throws Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    public Object createLCR() throws Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    public void processChunk(Object arg0) throws Throwable {
        // TODO Auto-generated method stub
        System.out.println("in process chunk");
    }

    public void processLCR(Object alcr) throws Throwable {
        // TODO Auto-generated method stub
        System.out.println("in process lcr");
        // JsonObject jsonObj = processLCR.lcr2ff(alcr);
        try {
            JsonObject jsonObj = new JsonObject();

            jsonObj.addProperty("timestamp", getTimeStamp(getValue(alcr, "getSourceTime").toString()));
            jsonObj.addProperty("database", getValue(alcr, "getSourceDatabaseName").toString());
            jsonObj.addProperty("schema", getValue(alcr, "getObjectOwner").toString());
            jsonObj.addProperty("table", getValue(alcr, "getObjectName").toString());
            String commandType = getValue(alcr, "getCommandType").toString();
            jsonObj.addProperty("cdc_type", commandType);
            jsonObj.addProperty("transactionId", getValue(alcr, "getTransactionId").toString());
            byte[] position = (byte[]) getValue(alcr, "getPosition");
            jsonObj.addProperty("position", new String(new Base32(true).encode(position)));
            JsonArray columns = new JsonArray();
            jsonObj.add("newValues", columns);
            for (Object columnValue : (Object[]) getValue(alcr, "getNewValues")) {
                JsonObject column = convert(columnValue);
                if (column != null) {
                    columns.add(column);
                }
            }
            columns = new JsonArray();
            jsonObj.add("oldValues", columns);
            for (Object columnValue : (Object[]) getValue(alcr, "getOldValues")) {
                JsonObject column = convert(columnValue);
                if (column != null) {
                    columns.add(column);
                }
            }

            System.out.println(jsonObj.toString());

            // String cdc_type = jsonObj.get("cdc_type").getAsString();
            switch (commandType) {
            case "INSERT":
                handler.inserts(jsonObj.toString(), position);
                break;
            case "UPDATE":
                handler.updates(jsonObj.toString(), position);
                break;
            case "DELETE":
                handler.deletes(jsonObj.toString(), position);
                break;
            default:
                handler.other(jsonObj.toString(), position);
                break;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new ProcessException("error creating json message" + ex.getMessage());
        }

    }

    public byte[] getFinalPosition() {
        return this.finalPosition;
    }

    public boolean isMoveWaterMark() {
        return this.moveWatermark;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // TODO Auto-generated method stub
        switch (method.getName()) {
        case "processLCR":
            if (Class.forName("oracle.streams.RowLCR", false, this.classLoader).isAssignableFrom(args[0].getClass()))
                processLCR(args[0]);
            break;
        }
        return null;
    }

    protected final JsonObject convert(Object value) throws SQLException, Throwable {
        Object datum = getColumnValue(value, "getColumnData");
        Class columnValue = Class.forName("oracle.streams.ColumnValue", false, this.classLoader);

        JsonObject column = new JsonObject();
        if (null == datum) {
            return null;
        }

        column.addProperty("name", getColumnValue(value, "getColumnName").toString());
        column.addProperty("oracleType", getColumnValue(value, "getColumnDataType").toString());
        int dataType = (int) getColumnValue(value, "getColumnDataType");
        DatumParser parser = new DatumParser(datum, this.classLoader);
        switch (dataType) {
        case 101:
            column.addProperty("type", "double");
            column.addProperty("value", parser.doubleValue());
            break;
        case 100:
            column.addProperty("type", "float");
            column.addProperty("value", parser.floatValue());
            break;
        case 1:
            column.addProperty("type", "String");
            column.addProperty("value", parser.stringValue());
            break;
        case 12:
            column.addProperty("type", "Date");
            column.addProperty("value", new Date(parser.timeStampValue(Calendar.getInstance())).toGMTString());
            break;
        case 2:
            column.addProperty("type", "BigDecimal");
            column.addProperty("value", parser.bigDecimalValue());
            break;
        // case 231:
        // value = convertTimestampLTZ(changeKey, datum);
        // break;
        // case 100:
        // value = convertTimestampTZ(changeKey, datum);
        // break;
        default:
            column.addProperty("type", "String");
            column.addProperty("value", parser.stringValue());
        }
        return column;
    }

    protected Object getValue(Object row, String methodName) throws Throwable {
        Class<?> rowLCR = Class.forName("oracle.streams.RowLCR", false, this.classLoader);
        Method method = rowLCR.getMethod(methodName);
        return method.invoke(row);
    }

    protected long getTimeStamp(String timeStamp) throws Throwable {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = format.parse(timeStamp);
        return date.getTime();
    }

    protected Object getColumnValue(Object columnValue, String methodName) throws Throwable {
        Class<?> columnCls = Class.forName("oracle.streams.ColumnValue", false, this.classLoader);
        Method method = columnCls.getMethod(methodName);
        return method.invoke(columnValue);
    }

    protected int getTypeConstant(String typeName) throws Throwable {
        Class<?> columnCls = Class.forName("oracle.streams.ColumnValue", false, this.classLoader);
        return columnCls.getDeclaredField(typeName).getInt(null);

    }

}
