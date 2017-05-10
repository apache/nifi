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
package org.apache.nifi.leaderelection.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZookeperElectionServiceTests {
    final String electionName = "/goatElection";
    final Integer timeout = 1000;
    ZookeeperElectionService service1;
    ZookeeperElectionService service2;
    TestingServer zkTestServer;
    CuratorFramework client;
    String zkhosts = "localhost:2181";
    @Before
    public void before() throws Exception{
        zkTestServer = new TestingServer(2181, true);
        zkTestServer.start();
        zkhosts = zkTestServer.getConnectString();
        client = CuratorFrameworkFactory.builder()
                .namespace("")
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        client.start();
        client.blockUntilConnected();
    }
    @After
    public void stopZkServer() throws IOException {
        zkTestServer.close();
    }
    @Test
    public void ValidServiceConnection() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, electionName);
        runner.enableControllerService(service1);
        runner.assertValid(service1);
    }
    @Test
    public void InvalidServiceConnection() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, electionName);
        runner.assertNotValid(service1);
    }
    @Test
    public void isTheLeaderTest() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, electionName);
        runner.enableControllerService(service1);
        assertTrue(service1.isLeader());
    }
    @Test
    public void isNotTheLeaderTest() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        service2 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, electionName);
        runner.enableControllerService(service1);
        runner.addControllerService("goatElection", service2);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, electionName);
        runner.enableControllerService(service2);
        assertTrue(service1.isLeader());
        assertFalse(service2.isLeader());
    }
    @Test
    public void multidepthZNodePathTest() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/goat/bear/man");
        runner.enableControllerService(service1);
        assertTrue(service1.isLeader());
    }
    @Test
    public void zNodePathCharTest() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
        service1 = new ZookeeperElectionService();
        runner.addControllerService("goatElection", service1);
        runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
        runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/111");
        runner.assertValid(service1);
        runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/111/");
        runner.assertNotValid(service1);
    }
    @Test(expected=java.lang.InterruptedException.class)
    public void properZNodeCountOnDisable() throws Exception {
         final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
         service1 = new ZookeeperElectionService();
         runner.addControllerService("goatElection", service1);
         runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
         runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
         runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/111");
         runner.enableControllerService(service1);
         runner.disableControllerService(service1);
         List<String>  kids = client.getChildren().forPath("/electionmulti/111");
         Stat  rootStillExists = client.checkExists().forPath("/electionmulti/111");
         assertTrue(kids.size() == 0);
         assertTrue(rootStillExists != null);
     }
    @Test(expected=java.lang.InterruptedException.class)
    public void disableLeadershipChanges() throws Exception {
         final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
         service1 = new ZookeeperElectionService();
         runner.addControllerService("goatElection", service1);
         runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
         runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
         runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/333");
         runner.enableControllerService(service1);
         service2 = new ZookeeperElectionService();
         runner.addControllerService("goatElection2", service2);
         runner.setProperty(service2, ZookeeperElectionService.zkHosts, zkhosts);
         runner.setProperty(service2, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
         runner.setProperty(service2, ZookeeperElectionService.zkElectionNode, "/electionmulti/333");
         runner.enableControllerService(service2);
         assertTrue(service1.isLeader());
         assertTrue(service1.aliveElectors().size() == 2);
         runner.disableControllerService(service1);
         Thread.sleep(100); //'Watch Based'
         assertTrue(service2.isLeader());
         runner.enableControllerService(service1);
         assertFalse(service1.isLeader());
         runner.disableControllerService(service2);
         Thread.sleep(100); //'Watch Based'
         assertTrue(service1.isLeader());
    }
    @Test
    public void ConnectionLossChanges() throws Exception{
    	 final TestRunner runner = TestRunners.newTestRunner(ZookeeperElectionTestProcessor.class);
         service1 = new ZookeeperElectionService();
         runner.addControllerService("goatElection", service1);
         runner.setProperty(service1, ZookeeperElectionService.zkHosts, zkhosts);
         runner.setProperty(service1, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
         runner.setProperty(service1, ZookeeperElectionService.zkElectionNode, "/electionmulti/333");
         runner.enableControllerService(service1);
         service2 = new ZookeeperElectionService();
         runner.addControllerService("goatElection2", service2);
         runner.setProperty(service2, ZookeeperElectionService.zkHosts, zkhosts);
         runner.setProperty(service2, ZookeeperElectionService.sessionTimeout, String.valueOf(timeout));
         runner.setProperty(service2, ZookeeperElectionService.zkElectionNode, "/electionmulti/333");
         runner.enableControllerService(service2);
    	String id1 = service1.ID();
    	String id2 = service2.ID();
    	zkTestServer.stop();
    	Thread.sleep(4000);
    	zkTestServer.start();
    	
    	assertFalse(id1.equalsIgnoreCase(service1.ID()));
    	assertFalse(id2.equalsIgnoreCase(service2.ID()));

    }
}