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
package org.apache.nifi.processors.hive;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHiveParser extends AbstractHiveQLProcessor {

    @Before
    public void initialize() {
        final MockProcessContext processContext = new MockProcessContext(this);
        final ProcessorInitializationContext initializationContext = new MockProcessorInitializationContext(this, processContext);
        initialize(initializationContext);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

    }

    @Test
    public void parseSelect() {
        String query = "select a.empid, to_something(b.saraly) from " +
                "company.emp a inner join default.salary b where a.empid = b.empid";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(new TableName("company", "emp", true)));
        assertTrue(tableNames.contains(new TableName("default", "salary", true)));
    }

    @Test
    public void parseSelectPrepared() {
        String query = "select empid from company.emp a where a.firstName = ?";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName("company", "emp", true)));
    }


    @Test
    public void parseLongSelect() {
        String query = "select\n" +
                "\n" +
                "    i_item_id,\n" +
                "\n" +
                "    i_item_desc,\n" +
                "\n" +
                "    s_state,\n" +
                "\n" +
                "    count(ss_quantity) as store_sales_quantitycount,\n" +
                "\n" +
                "    avg(ss_quantity) as store_sales_quantityave,\n" +
                "\n" +
                "    stddev_samp(ss_quantity) as store_sales_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(ss_quantity) / avg(ss_quantity) as store_sales_quantitycov,\n" +
                "\n" +
                "    count(sr_return_quantity) as store_returns_quantitycount,\n" +
                "\n" +
                "    avg(sr_return_quantity) as store_returns_quantityave,\n" +
                "\n" +
                "    stddev_samp(sr_return_quantity) as store_returns_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(sr_return_quantity) / avg(sr_return_quantity) as store_returns_quantitycov,\n" +
                "\n" +
                "    count(cs_quantity) as catalog_sales_quantitycount,\n" +
                "\n" +
                "    avg(cs_quantity) as catalog_sales_quantityave,\n" +
                "\n" +
                "    stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitycov\n" +
                "\n" +
                "from\n" +
                "\n" +
                "    store_sales,\n" +
                "\n" +
                "    store_returns,\n" +
                "\n" +
                "    catalog_sales,\n" +
                "\n" +
                "    date_dim d1,\n" +
                "\n" +
                "    date_dim d2,\n" +
                "\n" +
                "    date_dim d3,\n" +
                "\n" +
                "    store,\n" +
                "\n" +
                "    item\n" +
                "\n" +
                "where\n" +
                "\n" +
                "    d1.d_quarter_name = '2000Q1'\n" +
                "\n" +
                "        and d1.d_date_sk = ss_sold_date_sk\n" +
                "\n" +
                "        and i_item_sk = ss_item_sk\n" +
                "\n" +
                "        and s_store_sk = ss_store_sk\n" +
                "\n" +
                "        and ss_customer_sk = sr_customer_sk\n" +
                "\n" +
                "        and ss_item_sk = sr_item_sk\n" +
                "\n" +
                "        and ss_ticket_number = sr_ticket_number\n" +
                "\n" +
                "        and sr_returned_date_sk = d2.d_date_sk\n" +
                "\n" +
                "        and d2.d_quarter_name in ('2000Q1' , '2000Q2', '2000Q3')\n" +
                "\n" +
                "        and sr_customer_sk = cs_bill_customer_sk\n" +
                "\n" +
                "        and sr_item_sk = cs_item_sk\n" +
                "\n" +
                "        and cs_sold_date_sk = d3.d_date_sk\n" +
                "\n" +
                "        and d3.d_quarter_name in ('2000Q1' , '2000Q2', '2000Q3')\n" +
                "\n" +
                "group by i_item_id , i_item_desc , s_state\n" +
                "\n" +
                "order by i_item_id , i_item_desc , s_state\n" +
                "\n" +
                "limit 100";

        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(6, tableNames.size());
        AtomicInteger cnt = new AtomicInteger(0);
        for (TableName tableName : tableNames) {
            if (tableName.equals(new TableName(null, "store_sales", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "store_returns", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "catalog_sales", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "date_dim", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "store", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "item", true))) {
                cnt.incrementAndGet();
            }
        }
        assertEquals(6, cnt.get());
    }

    @Test
    public void parseSelectInsert() {
        String query = "insert into databaseA.tableA select key, max(value) from databaseA.tableA where category = 'x'";

        // The same database.tableName can appear two times for input and output.
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(2, tableNames.size());
        AtomicInteger cnt = new AtomicInteger(0);
        tableNames.forEach(tableName -> {
            if (tableName.equals(new TableName("databaseA", "tableA", false))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName("databaseA", "tableA", true))) {
                cnt.incrementAndGet();
            }
        });
        assertEquals(2, cnt.get());
    }

    @Test
    public void parseInsert() {
        String query = "insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id";

        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(3, tableNames.size());
        AtomicInteger cnt = new AtomicInteger(0);
        tableNames.forEach(tableName -> {
            if (tableName.equals(new TableName("databaseB", "tableB1", false))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "tableA1", true))) {
                cnt.incrementAndGet();
            } else if (tableName.equals(new TableName(null, "tableA2", true))) {
                cnt.incrementAndGet();
            }
        });
        assertEquals(3, cnt.get());
    }

    @Test
    public void parseUpdate() {
        String query = "update table_a set y = 'updated' where x > 100";

        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "table_a", false)));
    }

    @Test
    public void parseDelete() {
        String query = "delete from table_a where x > 100";

        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "table_a", false)));
    }

    @Test
    public void parseDDL() {
        String query = "CREATE TABLE IF NOT EXISTS EMPLOYEES(\n" +
                "EmployeeID INT,FirstName STRING, Title STRING,\n" +
                "State STRING, Laptop STRING)\n" +
                "COMMENT 'Employee Names'\n" +
                "STORED AS ORC";


        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "EMPLOYEES", false)));
    }

    @Test
    public void parseSetProperty() {
        String query = " set 'hive.exec.dynamic.partition.mode'=nonstrict";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(0, tableNames.size());
    }

    @Test
    public void parseSetRole() {
        String query = "set role all";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(0, tableNames.size());
    }

    @Test
    public void parseShowRoles() {
        String query = "show roles";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(0, tableNames.size());
    }

    @Test
    public void parseMsck() {
        String query = "msck repair table table_a";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "table_a", false)));
    }

    @Test
    public void parseAddJar() {
        String query = "ADD JAR hdfs:///tmp/my_jar.jar";
        final Set<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(0, tableNames.size());
    }

}
