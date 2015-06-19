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
package org.apache.nifi.dbcp;

@Deprecated
public class DatabaseSystems {

    /**
     * Currently contain only few known Database systems.
     * Please help to expand this list.
     *
     * Please be ensure that all JDBC drivers are license-compatible with Apache.
     * http://www.apache.org/legal/resolved.html
     * If not include them in "JDBC driver jar must be loaded from external location" section
     * and do not include actual driver in NiFi distribution (don't include driver in pom.xml file)
     *
     * {0} host name/ip
     * {1} port number
     * {2} database name
     *
     * for example url template
     *   "jdbc:postgresql://{0}:{1}/{2}"
     * will be after building
     *  "jdbc:postgresql://bighost:5432/Trove"
     */
    public static DatabaseSystemDescriptor[] knownDatabaseSystems = {
        // =================  JDBC driver jar should be included in nar (in pom.xml dependencies) =======================

        new DatabaseSystemDescriptor("Postgres", "PostgreSQL open soure object-relational database.",
            "org.postgresql.Driver", 5432, "jdbc:postgresql://{0}:{1}/{2}", true),

            new DatabaseSystemDescriptor("JavaDB", "Java DB is Oracle's supported distribution of the Apache Derby open source database. Included in JDK.",
                "org.apache.derby.jdbc.EmbeddedDriver", 1, "jdbc:derby:{2};create=true", true),

                new DatabaseSystemDescriptor("Derby", "Apache Derby is an open source relational database.",
                    "org.apache.derby.jdbc.EmbeddedDriver", 1, "jdbc:derby:{2};create=true", true),


                    // =================  JDBC driver jar must be loaded from external location  =======================
                    // Such drivers cannot be included in NiFi distribution because are not license-compatible with Apache.
                    new DatabaseSystemDescriptor("MariaDB",
                        "MariaDB is a community-developed fork of the MySQL relational database management system intended to remain free under the GNU GPL.",
                        "org.mariadb.jdbc.Driver", 3306, "jdbc:mariadb://{0}:{1}/{2}", false),

                        new DatabaseSystemDescriptor("Oracle",
                            "Oracle Database is an object-relational database management system.",
                            "oracle.jdbc.OracleDriver", 1521, "jdbc:oracle:thin:@//{0}:{1}/{2}", false),

                            new DatabaseSystemDescriptor("Sybase",
                                "Sybase is an relational database management system.",
                                "com.sybase.jdbc3.jdbc.SybDriver", 5000, "jdbc:sybase:Tds:{0}:{1}/{2}", false),


                                // =================  Unknown JDBC driver, user must provide connection details =====================
                                new DatabaseSystemDescriptor("Other DB", "Other JDBC compliant JDBC driver",
                                    null, 1, null, false),

    };

    public static DatabaseSystemDescriptor getDescriptor(String name) {
        for ( DatabaseSystemDescriptor descr : DatabaseSystems.knownDatabaseSystems) {
            if (descr.getValue().equalsIgnoreCase(name))
                return descr;
        }
        throw new IllegalArgumentException("Can't find DatabaseSystemDescriptor by name " + name);
    }


}
