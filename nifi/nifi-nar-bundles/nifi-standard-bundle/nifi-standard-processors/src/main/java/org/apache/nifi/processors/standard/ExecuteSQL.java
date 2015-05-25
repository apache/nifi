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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import org.apache.nifi.util.StopWatch;

@EventDriven
@Tags({"sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Execute provided SQL select query. Query result will be converted to Avro format."
		+ " Streaming is used so arbitrarily large result sets are supported.")
public class ExecuteSQL extends AbstractProcessor {

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
    		.name("failure")
    		.description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
    		.build();
    private final Set<Relationship> relationships;
    
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
    		.name("Database Connection Pooling Service")
    		.description("The Controller Service that is used to obtain connection to database")
    		.required(true)
    		.identifiesControllerService(DBCPService.class)
    		.build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
    		.name("SQL select query")
    		.description("SQL select query")
    		.required(true)
    		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    		.expressionLanguageSupported(true)
    		.build();

    private final List<PropertyDescriptor> propDescriptors;

    public ExecuteSQL() {
        HashSet<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        ArrayList<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(SQL_SELECT_QUERY);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String selectQuery = context.getProperty(SQL_SELECT_QUERY).getValue();
        
        try {
			final Connection con = dbcpService.getConnection();
			final Statement st = con.createStatement();
			
			final StopWatch stopWatch = new StopWatch(true);
			
			flowFile = session.write(flowFile, new OutputStreamCallback() {
				@Override
				public void process(final OutputStream out) throws IOException {
					try {
						ResultSet resultSet = st.executeQuery(selectQuery);
				        long nrOfRows = JdbcCommon.convertToAvroStream(resultSet, out);
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});

			logger.info("Transferred {} to 'success'", new Object[]{flowFile});
			session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
			session.transfer(flowFile, REL_SUCCESS);

		} catch (FlowFileAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
