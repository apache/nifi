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


import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

import java.sql.Connection;
import java.util.Map;

@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store" })
@CapabilityDescription("Provides a DBCPService that can be used to dynamically select another DBCPService. This service " +
        "requires an attribute named 'database.name' to be passed in when asking for a connection, and will throw an exception " +
        "if the attribute is missing. The value of 'database.name' will be used to select the DBCPService that has been " +
        "registered with that name. This will allow multiple DBCPServices to be defined and registered, and then selected " +
        "dynamically at runtime by tagging flow files with the appropriate 'database.name' attribute.")
@DynamicProperty(name = "The name to register DBCPService", value = "The DBCPService",
        description = "If '"+ DBCPConnectionPoolLookup.DATABASE_NAME_ATTRIBUTE +"' attribute contains " +
                "the name of the dynamic property, then the DBCPService (registered in the value) will be selected.",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class DBCPConnectionPoolLookup
        extends AbstractSingleAttributeBasedControllerServiceLookup<DBCPService> implements DBCPService {

    public static final String DATABASE_NAME_ATTRIBUTE = "database.name";

    @Override
    protected String getLookupAttribute() {
        return DATABASE_NAME_ATTRIBUTE;
    }

    @Override
    public Class<DBCPService> getServiceType() {
        return DBCPService.class;
    }

    @Override
    public Connection getConnection() throws ProcessException {
        throw new UnsupportedOperationException("Cannot lookup DBCPConnectionPool without attributes");
    }

    @Override
    public Connection getConnection(Map<String, String> attributes) {
        return lookupService(attributes).getConnection(attributes);
    }
}
