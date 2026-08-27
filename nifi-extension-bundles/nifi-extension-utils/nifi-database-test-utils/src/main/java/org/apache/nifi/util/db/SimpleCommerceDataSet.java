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
package org.apache.nifi.util.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * A sample data set for test consists of 'persons', 'products' and 'relationships' tables.
 */
public class SimpleCommerceDataSet {

    static final String DROP_PERSONS = "drop table persons";
    static final String DROP_PRODUCTS = "drop table products";
    static final String DROP_RELATIONSHIPS = "drop table relationships";
    static final String CREATE_PERSONS = "create table persons (id integer, name varchar(100), code integer)";
    static final String CREATE_PRODUCTS = "create table products (id integer, name varchar(100), code integer)";
    static final String CREATE_RELATIONSHIPS = "create table relationships (id integer,name varchar(100), code integer)";

    public static void loadTestData2Database(Connection con, int nrOfPersons, int nrOfProducts, int nrOfRels) throws SQLException {

        System.out.println(createRandomName());
        System.out.println(createRandomName());
        System.out.println(createRandomName());

        final Statement st = con.createStatement();

        // tables may not exist, this is not serious problem.
        try {
            st.executeUpdate(DROP_PERSONS);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(DROP_PRODUCTS);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(DROP_RELATIONSHIPS);
        } catch (final Exception ignored) {
        }

        st.executeUpdate(CREATE_PERSONS);
        st.executeUpdate(CREATE_PRODUCTS);
        st.executeUpdate(CREATE_RELATIONSHIPS);

        for (int i = 0; i < nrOfPersons; i++) {
            loadPersons(st, i);
        }

        for (int i = 0; i < nrOfProducts; i++) {
            loadProducts(st, i);
        }

        for (int i = 0; i < nrOfRels; i++) {
            loadRelationships(st, i);
        }

        st.close();
    }

    static final Random RNG = new Random(53495);

    private static void loadPersons(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into persons values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static void loadProducts(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into products values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static void loadRelationships(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into relationships values (" + nr + ", '" + createRandomName() + "', " + RNG.nextInt(469946) + ")");
    }

    private static String createRandomName() {
        return createRandomString() + " " + createRandomString();
    }

    private static String createRandomString() {

        final int length = RNG.nextInt(10);
        final String characters = "ABCDEFGHIJ";

        final char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(RNG.nextInt(characters.length()));
        }
        return new String(text);
    }
}
