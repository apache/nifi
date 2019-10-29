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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;

public class DatumParser {

    private Object datum;
    private Class<?> datumCls;
    private ClassLoader classLoader;

    public DatumParser(Object datum, ClassLoader classLoader) throws Exception {
        this.datum = datum;
        this.datumCls = Class.forName("oracle.sql.Datum", false, classLoader);
        this.classLoader = classLoader;
    }

    public double doubleValue() throws Exception {
        Method method = datumCls.getMethod("doubleValue");
        return (double) method.invoke(datum);
    }

    public String stringValue() throws Exception {
        Method method = datumCls.getMethod("stringValue");
        return (String) method.invoke(datum);
    }

    public float floatValue() throws Exception {
        Method method = datumCls.getMethod("floatValue");
        return (float) method.invoke(datum);
    }

    public BigDecimal bigDecimalValue() throws Exception {
        Method method = datumCls.getMethod("bigDecimalValue");
        return (BigDecimal) method.invoke(datum);
    }

    public long timeStampValue() throws Exception {
        Method method = datumCls.getMethod("timeStampValue");
        Timestamp ts = (Timestamp) method.invoke(datum);
        return ts.getTime();
    }

    public long timeStampValue(Calendar cal) throws Exception {
        Method method = datumCls.getMethod("timeStampValue", Calendar.class);
        Timestamp ts = (Timestamp) method.invoke(datum, cal);
        return ts.getTime();
    }

}
