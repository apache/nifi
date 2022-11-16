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
package org.apache.nifi.processors.iceberg.metastore;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.Files.createTempDirectory;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.AUTO_CREATE_ALL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECTION_DRIVER;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_TXN_MANAGER;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.SCHEMA_VERIFICATION;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.THRIFT_URIS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.WAREHOUSE;

/** This class wraps Metastore service core functionalities. */
class MetastoreCore {

    private final String DB_NAME = "test_metastore";

    private String thriftConnectionUri;
    private Configuration hiveConf;
    private HiveMetaStoreClient metastoreClient;
    private File tempDir;
    private ExecutorService thriftServer;
    private TServer server;

    public void initialize() throws IOException, TException, InvocationTargetException, NoSuchMethodException,
            IllegalAccessException, NoSuchFieldException, SQLException {
        thriftServer = Executors.newSingleThreadExecutor();
        tempDir = createTempDirectory("metastore").toFile();
        setDerbyLogPath();
        setupDB("jdbc:derby:" + getDerbyPath() + ";create=true");

        server = thriftServer();
        thriftServer.submit(() -> server.serve());

        metastoreClient = new HiveMetaStoreClient(hiveConf);
        metastoreClient.createDatabase(new Database(DB_NAME, "description", getDBPath(), new HashMap<>()));
    }

    public void shutdown() {

        metastoreClient.close();

        if (server != null) {
            server.stop();
        }

        thriftServer.shutdown();

        if (tempDir != null) {
            tempDir.delete();
        }
    }

    private HiveConf hiveConf(int port) {
        thriftConnectionUri = "thrift://localhost:" + port;

        final HiveConf hiveConf = new HiveConf(new Configuration(), this.getClass());
        hiveConf.set(THRIFT_URIS.getVarname(), thriftConnectionUri);
        hiveConf.set(WAREHOUSE.getVarname(), "file:" + tempDir.getAbsolutePath());
        hiveConf.set(WAREHOUSE.getHiveName(), "file:" + tempDir.getAbsolutePath());
        hiveConf.set(CONNECTION_DRIVER.getVarname(), EmbeddedDriver.class.getName());
        hiveConf.set(CONNECT_URL_KEY.getVarname(), "jdbc:derby:" + getDerbyPath() + ";create=true");
        hiveConf.set(AUTO_CREATE_ALL.getVarname(), "false");
        hiveConf.set(SCHEMA_VERIFICATION.getVarname(), "false");
        hiveConf.set(HIVE_TXN_MANAGER.getVarname(), "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        hiveConf.set(COMPACTOR_INITIATOR_ON.getVarname(), "true");
        hiveConf.set(COMPACTOR_WORKER_THREADS.getVarname(), "1");
        hiveConf.set(HIVE_SUPPORT_CONCURRENCY.getVarname(), "true");
        hiveConf.setBoolean("hcatalog.hive.client.cache.disabled", true);


        hiveConf.set(CONNECTION_POOLING_TYPE.getVarname(), "NONE");
        hiveConf.set(HMS_HANDLER_FORCE_RELOAD_CONF.getVarname(), "true");

        return hiveConf;
    }

    private void setDerbyLogPath() throws IOException {
        final String derbyLog = Files.createTempFile(tempDir.toPath(), "derby", ".log").toString();
        System.setProperty("derby.stream.error.file", derbyLog);
    }

    private String getDerbyPath() {
        return new File(tempDir, "metastore_db").getPath();
    }

    private TServer thriftServer() throws TTransportException, MetaException, InvocationTargetException,
            NoSuchMethodException, IllegalAccessException, NoSuchFieldException {
        final TServerSocketKeepAlive socket = new TServerSocketKeepAlive(new TServerSocket(0));
        hiveConf = hiveConf(socket.getServerSocket().getLocalPort());
        final HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", hiveConf);
        final IHMSHandler handler = RetryingHMSHandler.getProxy(hiveConf, baseHandler, true);
        final TTransportFactory transportFactory = new TTransportFactory();
        final TSetIpAddressProcessor<IHMSHandler> processor = new TSetIpAddressProcessor<>(handler);

        TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
                .processor(processor)
                .transportFactory(transportFactory)
                .protocolFactory(new TBinaryProtocol.Factory())
                .minWorkerThreads(3)
                .maxWorkerThreads(5);

        return new TThreadPoolServer(args);
    }

    private void setupDB(String dbURL) throws SQLException, IOException {
        final Connection connection = DriverManager.getConnection(dbURL);
        ScriptRunner scriptRunner = new ScriptRunner(connection);

        final URL initScript = getClass().getClassLoader().getResource("hive-schema-4.0.0-alpha-2.derby.sql");
        final Reader reader = new BufferedReader(new FileReader(initScript.getFile()));
        scriptRunner.runScript(reader);
    }

    private String getDBPath() {
        return Paths.get(tempDir.getAbsolutePath(), DB_NAME + ".db").toAbsolutePath().toString();
    }

    public String getThriftConnectionUri() {
        return thriftConnectionUri;
    }

    public String getWarehouseLocation() {
        return tempDir.getAbsolutePath();
    }

}

