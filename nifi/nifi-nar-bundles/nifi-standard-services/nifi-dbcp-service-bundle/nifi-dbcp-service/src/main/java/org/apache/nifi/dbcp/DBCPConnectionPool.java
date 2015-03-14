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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
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
public class DBCPConnectionPool extends AbstractControllerService implements DBCPService {

	public static final DatabaseSystemDescriptor DEFAULT_DATABASE_SYSTEM = DatabaseSystems.getDescriptor("JavaDB");

    public static final PropertyDescriptor DATABASE_SYSTEM = new PropertyDescriptor.Builder()
    .name("Database")
    .description("Database management system")
    .allowableValues(DatabaseSystems.knownDatabaseSystems)
    .defaultValue(DEFAULT_DATABASE_SYSTEM.getValue())
    .required(true)
    .build();    

    public static final PropertyDescriptor DB_HOST = new PropertyDescriptor.Builder()
    .name("Database Host")
    .description("Database Host")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_PORT = new PropertyDescriptor.Builder()
    .name("Database Port")
    .description("Database server port")
    .defaultValue(DEFAULT_DATABASE_SYSTEM.defaultPort.toString())
    .required(true)
    .addValidator(StandardValidators.PORT_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
    .name("Database Driver Class Name")
    .description("Database driver class name")
    .defaultValue(DEFAULT_DATABASE_SYSTEM.driverClassName)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_DRIVER_JAR_URL = new PropertyDescriptor.Builder()
    .name("Database Driver Jar Url")
    .description("Optional database driver jar file path url. For example 'file:///var/tmp/mariadb-java-client-1.1.7.jar'")
    .defaultValue(null)
    .required(false)
    .addValidator(StandardValidators.URL_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
    .name("Database Name")
    .description("Database name")
    .defaultValue(null)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
    .name("Database User")
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

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
    .name("Max Wait Time")
    .description("The maximum amount of time that the pool will wait (when there are no available connections) " 
     + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
    .defaultValue("500 millis")
    .required(true)
    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
    .sensitive(false)
    .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
    .name("Max Total Connections")
    .description("The maximum number of active connections that can be allocated from this pool at the same time, " 
     + " or negative for no limit.")
    .defaultValue("8")
    .required(true)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .sensitive(true)
    .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_SYSTEM);
        props.add(DB_HOST);
        props.add(DB_PORT);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_JAR_URL);
        props.add(DB_NAME);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        
        properties = Collections.unmodifiableList(props);
    }
    
    private volatile BasicDataSource dataSource;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     *  Create new pool, open some connections ready to be used
     * @param context
     * @throws InitializationException
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        DatabaseSystemDescriptor dbsystem = DatabaseSystems.getDescriptor( context.getProperty(DATABASE_SYSTEM).getValue() );
        
        String host   = context.getProperty(DB_HOST).getValue();
        Integer port  = context.getProperty(DB_PORT).asInteger();
        String drv    = context.getProperty(DB_DRIVERNAME).getValue();
        String dbname = context.getProperty(DB_NAME).getValue();
        String user   = context.getProperty(DB_USER).getValue();
        String passw  = context.getProperty(DB_PASSWORD).getValue();
        Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        Integer maxTotal  = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();
        
        dataSource = new BasicDataSource();        
        dataSource.setDriverClassName(drv);
        
        // Optional driver URL, when exist, this URL will be used to locate driver jar file location
        String urlString	= context.getProperty(DB_DRIVER_JAR_URL).getValue();
        dataSource.setDriverClassLoader( getDriverClassLoader(urlString, drv) );
        
        String dburl  = dbsystem.buildUrl(host, port, dbname);
        
        dataSource.setMaxWait(maxWaitMillis);
        dataSource.setMaxActive(maxTotal);

        dataSource.setUrl(dburl);
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
    
    /**	
     * 	using Thread.currentThread().getContextClassLoader();
     * will ensure that you are using the ClassLoader for you NAR.
     * @throws InitializationException 
     */
    protected ClassLoader getDriverClassLoader(String urlString, String drvName) throws InitializationException {
        if (urlString!=null && urlString.length()>0) {
        	try {
				URL[] urls = new URL[] { new URL(urlString) };
				URLClassLoader ucl = new URLClassLoader(urls);
				
				// Workaround which allows to use URLClassLoader for JDBC driver loading.
				// (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.) 
		    	Class<?> clazz = Class.forName(drvName, true, ucl);
		    	if (clazz==null)
		    		throw new InitializationException("Can't load Database Driver " + drvName);
		    	Driver driver = (Driver) clazz.newInstance();
		    	DriverManager.registerDriver( new DriverShim(driver) );
				
				return ucl;
				
			} catch (MalformedURLException e) {
				throw new InitializationException("Invalid Database Driver Jar Url", e);
			} catch (Exception e) {
				throw new InitializationException("Can't load Database Driver", e);
			}
        }
        else 
            // That will ensure that you are using the ClassLoader for you NAR. 
            return Thread.currentThread().getContextClassLoader();
    }

    /**
     *  Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
    	try {
			dataSource.close();
		} catch (SQLException e) {
			throw new ProcessException(e);
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
        return "DBCPConnectionPool[id=" + getIdentifier() + "]";
    }

}
