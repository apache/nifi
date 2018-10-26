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
package org.apache.nifi.marklogic.processor;

import java.io.OutputStream;
import java.io.Serializable;

import org.apache.nifi.processor.ProcessContext;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.Transaction;
import com.marklogic.client.DatabaseClientFactory.SecurityContext;
import com.marklogic.client.admin.ServerConfigurationManager;
import com.marklogic.client.alerting.RuleManager;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.extensions.ResourceManager;
import com.marklogic.client.impl.QueryManagerImpl;
import com.marklogic.client.pojo.PojoRepository;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.util.RequestLogger;

class TestQueryMarkLogic extends QueryMarkLogic {
    @Override
    public DatabaseClient getDatabaseClient(ProcessContext context) {
        return new TestMLDatabaseClient();
    }
}

class TestMLDatabaseClient implements DatabaseClient {

    @Override
    public Transaction openTransaction() throws ForbiddenUserException, FailedRequestException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Transaction openTransaction(String name) throws ForbiddenUserException, FailedRequestException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Transaction openTransaction(String name, int timeLimit)
            throws ForbiddenUserException, FailedRequestException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GenericDocumentManager newDocumentManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BinaryDocumentManager newBinaryDocumentManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JSONDocumentManager newJSONDocumentManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TextDocumentManager newTextDocumentManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public XMLDocumentManager newXMLDocumentManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataMovementManager newDataMovementManager() {
        // TODO Auto-generated method stub
        return new TestDataMovementManager();
    }

    @Override
    public QueryManager newQueryManager() {
        // TODO Auto-generated method stub
        return new QueryManagerImpl(null);
    }

    @Override
    public RowManager newRowManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RuleManager newRuleManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServerConfigurationManager newServerConfigManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GraphManager newGraphManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SPARQLQueryManager newSPARQLQueryManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T, ID extends Serializable> PojoRepository<T, ID> newPojoRepository(Class<T> clazz, Class<ID> idClass) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends ResourceManager> T init(String resourceName, T resourceManager) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RequestLogger newLogger(OutputStream out) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void release() {
        
    }

    @Override
    public Object getClientImplementation() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServerEvaluationCall newServerEval() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConnectionType getConnectionType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getHost() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getDatabase() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SecurityContext getSecurityContext() {
        // TODO Auto-generated method stub
        return null;
    }
    
}