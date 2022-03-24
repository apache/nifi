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
package org.apache.nifi.admin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;

/**
 * A utility class for useful methods dealing with the repository
 *
 */
public class RepositoryUtils {

    public static void rollback(final Connection conn, final Logger logger) {
        try {
            if (null != conn) {
                conn.rollback();
            }
        } catch (final SQLException sqe) {
            logger.warn("The following problem occurred while trying to rollback " + conn + ": " + sqe.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("", sqe);
            }
        }
    }

    /**
     * Closes the given statement quietly - no logging, no exceptions
     *
     * @param statement to close
     */
    public static void closeQuietly(final Statement statement) {

        if (null != statement) {
            try {
                statement.close();
            } catch (final SQLException se) { /*IGNORE*/

            }
        }
    }

    /**
     * Closes the given result set quietly - no logging, no exceptions
     *
     * @param resultSet to close
     */
    public static void closeQuietly(final ResultSet resultSet) {
        if (null != resultSet) {
            try {
                resultSet.close();
            } catch (final SQLException se) {/*IGNORE*/

            }
        }
    }

    /**
     * Closes the given connection quietly - no logging, no exceptions
     *
     * @param conn to close
     */
    public static void closeQuietly(final Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (final SQLException se) {/*IGNORE*/

            }
        }
    }

}
