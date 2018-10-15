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

package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"cassandra", "cql", "delete", "remove", "update", "change"})
@CapabilityDescription("Executes a delete or update CQL command against a Cassandra cluster. The command may "
        + "come from either an input FlowFile or from the Query configuration parameter. The CQL command may use "
        + "the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes with the "
        + "naming convention cql.args.N.type and cql.args.N.value, where N is a positive integer. The cql.args.N.type "
        + "is expected to be a lowercase string indicating the Cassandra type.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "cql.args.N.type",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The type of each "
                        + "parameter is specified as a lowercase string corresponding to the Cassandra data type (text, "
                        + "int, boolean, e.g.). In the case of collections, the primitive type(s) of the elements in the "
                        + "collection should be comma-delimited, follow the collection type, and be enclosed in angle brackets "
                        + "(< and >), for example set<text> or map<timestamp, int>."),
        @ReadsAttribute(attribute = "cql.args.N.value",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The value of the "
                        + "parameters are specified as cql.args.1.value, cql.args.2.value, cql.args.3.value, and so on. The "
                        + " type of the cql.args.1.value parameter is specified by the cql.args.1.type attribute.")
})
public class UpdateCassandra extends AbstractCassandraProcessor {
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("delete-cassandra-query")
        .displayName("Delete Query")
        .description("The query to execute. Cannot be set at the same time that the Table property is set.")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    private final static List<PropertyDescriptor> propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONTACT_POINTS, KEYSPACE, QUERY, CLIENT_AUTH, USERNAME, PASSWORD,
            CONSISTENCY_LEVEL, PROP_SSL_CONTEXT_SERVICE));

    private final static Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        try {
            connectToCassandra(context);
        } catch (NoHostAvailableException nhae) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this statement", nhae);
            getLogger().error(nhae.getCustomMessage(10, true, false));
            throw new ProcessException(nhae);
        } catch (AuthenticationException ae) {
            getLogger().error("Invalid username/password combination", ae);
            throw new ProcessException(ae);
        }
    }

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
            .name(name)
            .displayName(name)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        try {
            String query = null;
            if (context.getProperty(QUERY).isSet()) {
                query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
            } else if (input != null) {
                query = getQueryFromFlowFile(input, session);
            }

            final Session deleteSession = cassandraSession.get();

            PreparedStatement statement = deleteSession.prepare(query);
            BoundStatement boundStatement = statement.bind();
            if (input != null) {
                buildBoundStatement(input, boundStatement);
            }

            deleteSession.execute(boundStatement);

            if (input != null) {
                session.transfer(input, REL_SUCCESS);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
        }
    }

    private String getQueryFromFlowFile(FlowFile input, ProcessSession session) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        session.exportTo(input, out);
        out.close();

        return new String(out.toByteArray());
    }
}
