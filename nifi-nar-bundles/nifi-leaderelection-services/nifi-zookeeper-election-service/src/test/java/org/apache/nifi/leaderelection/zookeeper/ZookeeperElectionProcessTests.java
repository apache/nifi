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

import java.io.IOException;
import java.util.Collections;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;
@RunWith(JUnit4.class)
public class ZookeeperElectionProcessTests extends TestCase {
     final String electionName = "/electionTests";
     final Integer timeout = 5000;
     ZooKeeper masterTestZK;
     ZookeeperElectionProcess zep1;
     ZookeeperElectionProcess zep2;
     ZookeeperElectionProcess zep3;
     TestingServer zkTestServer;
     String zkhosts = "localhost:2181";
    protected void setUp() throws Exception {
        super.setUp();
    }
    @Before
    public void before() throws Exception{
        zkTestServer = new TestingServer(2181, true);
        zkTestServer.start();
        zkhosts = zkTestServer.getConnectString();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .namespace("my_namespace")
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 1))
                .build();
        client.start();
        client.blockUntilConnected();
    }
    @After
    public void stopZkServer() throws IOException {
        zkTestServer.close();
        }
    protected void tearDown() throws InterruptedException, KeeperException {
         zep1 = null;
         zep2 = null;
         zep3 = null;
         masterTestZK.delete(electionName, -1);
         masterTestZK.close();
         masterTestZK = null;
        }
    @Test
    public void SingleNodeLeaderRejoin() throws IOException, InterruptedException, KeeperException{
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        masterTestZK = new ZooKeeper(zkhosts, timeout, null);
        zep1.run();
        String id1 = zep1.ID();
        boolean leader1 = zep1.isLeader();
        masterTestZK.delete(id1, -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        String id2 = zep1.ID();
        boolean leader2 =  zep1.isLeader();
        if(id1 != id2 && leader1 == true && leader2 == true){
            assertTrue(true);
            }else {
            assertTrue(false);
        }
    }
    @Test
    public void TripletNodeLeaderLostRejoinOnce() throws IOException, InterruptedException, KeeperException {
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep2 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep3 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep1.run();
        zep2.run();
        zep3.run();
        masterTestZK = new ZooKeeper(zkhosts, timeout, null);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep1.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        assertTrue(zep2.isLeader());
        assertFalse(zep3.isLeader());
        assertFalse(zep1.isLeader());
    }
    @Test
    public void TripletNodeLeaderLostRejoinCircle() throws IOException, InterruptedException, KeeperException{
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep2 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep3 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep1.run();
        zep2.run();
        zep3.run();
        masterTestZK = new ZooKeeper(zkhosts, timeout, null);
        Thread.sleep(100L);//Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep1.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep2.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep3.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        assertTrue(zep1.isLeader());
        assertFalse(zep2.isLeader());
        assertFalse(zep3.isLeader());
    }
    @Test
    public void TripletNodeLeaderLostRejoinCircleReverse() throws IOException, InterruptedException, KeeperException{
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep2 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep3 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep1.run();
        zep2.run();
        zep3.run();
        masterTestZK = new ZooKeeper(zkhosts, timeout, null);
        Thread.sleep(100L);//Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep3.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep2.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        masterTestZK.delete(zep1.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        assertTrue(zep1.aliveElectors().size()==3);
        assertTrue(zep3.isLeader());
    }
    @Test
    public void AliveHostsChanges() throws InterruptedException, IOException, KeeperException {
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep1.run();
        Thread.sleep(100L);
        assertTrue(zep1.aliveElectors().size() == 1);
        zep2 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep2.run();
        Thread.sleep(100L);
        assertTrue(zep1.aliveElectors().size() == 2);
        zep3 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep3.run();
        Thread.sleep(100L);
        assertTrue(zep1.aliveElectors().size() == 3);
    }
    @Test
    public void LastElectionTests() throws IOException, InterruptedException, KeeperException {
        zep1 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep2 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep3 = new ZookeeperElectionProcess(electionName, zkhosts, timeout);
        zep1.run();
        zep2.run();
        zep3.run();
        masterTestZK = new ZooKeeper(zkhosts, timeout, null);
        long lastTime = masterTestZK.exists(electionName+"/"+Collections.min(masterTestZK.getChildren(electionName, false)), false).getCtime();
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        if ( lastTime == zep1.LastElection()
                && lastTime == zep2.LastElection()
                && lastTime == zep3.LastElection() )
            assertTrue(true);
        else{
            assertTrue(false);
        }
        masterTestZK.delete(zep1.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        lastTime = masterTestZK.exists(electionName+"/"+Collections.min(masterTestZK.getChildren(electionName, false)), false).getCtime();
        if ( lastTime == zep1.LastElection()
                && lastTime == zep2.LastElection()
                && lastTime == zep3.LastElection() )
            assertTrue(true);
        else{
            assertTrue(false);
        }
        masterTestZK.delete(zep2.ID(), -1);
        masterTestZK.delete(zep3.ID(), -1);
        Thread.sleep(100L); //Gotta give the watch more then a clock cycle to get over here!
        lastTime = masterTestZK.exists(electionName+"/"+Collections.min(masterTestZK.getChildren(electionName, false)), false).getCtime();
        if ( lastTime == zep1.LastElection()
                && lastTime == zep2.LastElection()
                && lastTime == zep3.LastElection() )
            assertTrue(true);
        else{
            assertTrue(false);
        }
    }
}