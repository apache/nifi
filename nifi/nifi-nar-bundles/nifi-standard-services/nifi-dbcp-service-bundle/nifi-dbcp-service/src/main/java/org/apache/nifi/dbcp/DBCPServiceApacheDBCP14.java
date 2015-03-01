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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

/**
 * Implementation of for Database Connection Pooling Service.
 * Apache DBCP is used for connection pooling functionality.
 *
 */
@Tags({"dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage."
        )
public class DBCPServiceApacheDBCP14 extends AbstractControllerService implements DBCPService {

	public static final DatabaseSystemDescriptor DEFAULT_DATABASE_SYSTEM = DatabaseSystems.getDescriptor("JavaDB");

    public static final PropertyDescriptor DATABASE_SYSTEM = new PropertyDescriptor.Builder()
    .name("Database")
    .description("Database management system")
//    .allowableValues(POSTGRES, JavaDB, DERBY, MariaDB, OtherDB)
    .allowableValues(DatabaseSystems.knownDatabaseSystems)
    .defaultValue(DEFAULT_DATABASE_SYSTEM.getValue())
    .required(true)
    .build();    

    public static final PropertyDescriptor DB_HOST = new PropertyDescriptor.Builder()
    .name("Database host")
    .description("Database host")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_PORT = new PropertyDescriptor.Builder()
    .name("Database port")
    .description("Database server port")
    .defaultValue(DEFAULT_DATABASE_SYSTEM.defaultPort.toString())
    .required(true)
    .addValidator(StandardValidators.PORT_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
    .name("Database driver class name")
    .description("Database driver class name")
    .defaultValue(DEFAULT_DATABASE_SYSTEM.driverClassName)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
    .name("Database name")
    .description("Database name")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
    .name("Database user")
    .description("Database user name")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
    .name("Password")
    .description("The password for the database user")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(true)
    .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_SYSTEM);
        props.add(DB_HOST);
        props.add(DB_PORT);
        props.add(DB_DRIVERNAME);
        props.add(DB_NAME);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        
        properties = Collections.unmodifiableList(props);
    }
    
    private ConfigurationContext configContext;
    private volatile BasicDataSource dataSource;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    //=================================   Apache DBCP pool parameters  ================================ 
    
    /** The maximum number of milliseconds that the pool will wait (when there are no available connections) 
     * for a connection to be returned before throwing an exception, or -1 to wait indefinitely. 
     */
    static final long maxWaitMillis = 500;
   
    /** The maximum number of active connections that can be allocated from this pool at the same time, 
     * or negative for no limit.
     */
    static final int maxTotal 		= 8;

    //=================================================================================================
    
    /**
     * Idea was to dynamically set port, driver and url properties default values after user select database system.
     * As of 01mar2015 such functionality is not supported.
     * 
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor.equals(DATABASE_SYSTEM)) {
        	
        	DatabaseSystemDescriptor databaseSystemDescriptor = DatabaseSystems.getDescriptor(newValue);
        }        
    }
	*/

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        configContext = context;

        DatabaseSystemDescriptor dbsystem = DatabaseSystems.getDescriptor( context.getProperty(DATABASE_SYSTEM).getValue() );
        
        String host   = context.getProperty(DB_HOST).getValue();
        Integer port  = context.getProperty(DB_PORT).asInteger();
        String drv    = context.getProperty(DB_DRIVERNAME).getValue();
        String dbname = context.getProperty(DB_NAME).getValue();
        String user   = context.getProperty(DB_USER).getValue();
        String passw  = context.getProperty(DB_PASSWORD).getValue();
        
        String dburl  = dbsystem.buildUrl(host, port, dbname);
        
        dataSource = new BasicDataSource();
        dataSource.setMaxWait(maxWaitMillis);
        dataSource.setMaxActive(maxTotal);

        dataSource.setUrl(dburl);
        dataSource.setDriverClassName(drv);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        // verify connection can be established.
        try {
			Connection con = dataSource.getConnection();
			if (con==null)
				throw new InitializationException("Connection to database cannot be established.");
			con.close();
		} catch (SQLException e) {
			throw new InitializationException(e);
		}
    }
    
	@Override
	public Connection getConnection() throws ProcessException {
		try {
			Connection con = dataSource.getConnection();
			return con;
		} catch (SQLException e) {
			throw new ProcessException(e);
		}
	}

    @Override
    public String toString() {
        return "DBCPServiceApacheDBCP14[id=" + getIdentifier() + "]";
    }

}
