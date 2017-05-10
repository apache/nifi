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
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperElectionProcess implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperElectionProcess.class);

    private final String DEFAULT_ZNODE;
    private final String ZK_SEQ = "/n_";

    private String watchedElector;
    private String electorID;
    private boolean isLeader = false;
    private final ElectionZookeeper ezk;

    public ZookeeperElectionProcess(String ElectionZNodeName, String ZKHosts, Integer timeout) throws IOException {
        DEFAULT_ZNODE = ElectionZNodeName;
        ezk = new ElectionZookeeper(ZKHosts, new ElectorWatcher(), timeout);
    }
    /**
     * This method should return a list of all currently participating peers
     * It could be for peer discovery or for for general internal leadership
     * tracking needs.
     * @return - List of alive electors
     */
    public List<String> aliveElectors() {
        return ezk.getZNodeChildren(DEFAULT_ZNODE);
    }
    /**
     * Return if the current elector is the leader or not
     * @return - If current leader return True, else False
     */
    public boolean isLeader() {
        return isLeader;
    }
    /**
     * long based gmt epoch of the last election, should return the znode ctime
     * of the lowest seq elector
     * @return long of epoch of last election
     */
    public long LastElection() {
        return ezk.getZNodeCreationTime(DEFAULT_ZNODE+"/"+Collections.min(ezk.getZNodeChildren(DEFAULT_ZNODE)));
    }
    public String ID(){
        return electorID;
    }
    /**
     * Have host join the election this includes making the default znode if
     * required for the election if we are first otherwise we need to start
     * watching the follower in front of us
     */
    private void joinElection() {
        List<String> voters = ezk.getZNodeChildren(DEFAULT_ZNODE);
        // use natural ordering to find the leader (lowest seq voter)
        Collections.sort(voters);
        if (electorID.endsWith(voters.get(0))|| voters.size() <= 1) {
            // we are leader - be the leader man go check yourself out!
            isLeader = true;
            logger.info("Is Now Election Leader " + electorID + " for Election at: " + DEFAULT_ZNODE);
        } else {
            // we are not leader - watch the guy in front of us and ourself... Setup the watchedElector variable for the ElectorWatcher
            watchedElector = DEFAULT_ZNODE+"/"+voters.get(voters.indexOf(electorID) + 1);
            ezk.watchZNode(watchedElector);
        }
        ezk.watchZNode(electorID);
        logger.debug(electorID + " has joined election " + DEFAULT_ZNODE);
    }
    @Override
    public void run() {
        try {
            ezk.createElectionAbsolutePath(DEFAULT_ZNODE);
            electorID = ezk.createZNode(DEFAULT_ZNODE + ZK_SEQ, true);
        } catch (Exception e){
            logger.error("Issue Creating Election Root or Inital Voter Node, destroying session with Zookeeper");
            destroy();
            throw new IllegalStateException(e.getMessage());
        }
        joinElection();
    }
    public void destroy() {
        ezk.cleanShutdown();
        Thread.currentThread().interrupt();;
    }
    // Internal class for managing the zK interface for the election.
    public class ElectionZookeeper {
        private ZooKeeper zK;
        public ElectionZookeeper(String connectionSting, final ElectorWatcher electorWatch, Integer timeout)
                throws IOException {
            zK = new ZooKeeper(connectionSting, timeout, electorWatch);
        }
        /**
         * We only need to create a root election node if not exists, and one
         * for our vote that is always EPH_SEQ
         * @param znodePath - path to create
         * @param ephimeral - should it be EPHEMERAL_SEQUENTIAL or not
         * @return - boolean created ZNode Path
         * @throws KeeperException -- could be ACL or already exists
         * @throws InterruptedException -- issues connecting
         */
        public String createZNode(final String znodePath, final boolean ephimeral) {
                String createdPath;
                try {
                    createdPath = zK.create(znodePath, null/* data */, Ids.OPEN_ACL_UNSAFE,
                            ephimeral ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT);
                } catch (KeeperException | InterruptedException e) {
                    throw new IllegalStateException(e.getMessage());
                }
                return createdPath;
        }
        public void createElectionAbsolutePath(final String znodePath){
            try{
                //Due to ZK Atomic Nature we are stuck with separate creates or multi-obj
                List<Op> ops = new LinkedList<Op>();
                String absoluteBuild = "";
                for(String part : DEFAULT_ZNODE.split("/")){
                    if(part.length() > 0){
                        absoluteBuild += "/"+part;
                        ops.add(Op.create(absoluteBuild, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                    }
                }
                zK.multi(ops);
                } catch (KeeperException.NodeExistsException e) {
                    //Dont Care - Can only occur for Root Election Node
                } catch (Exception e){
                    logger.error("Issue Creating Election Root or Inital Voter Node");
                    throw new IllegalStateException(e.getMessage());
                }
        }
        public void removeZNode(String znodePath){
            try{
                zK.delete(znodePath, -1);
            }catch (KeeperException.NoNodeException e){
                logger.debug("ZNode was expired before we attempted to cleanup");
            }catch (Exception e){
                logger.error("Failed to remove ZNode");
                throw new IllegalStateException(e.getMessage());
            }
        }
        public boolean watchZNode(String znodePath) {
            boolean watched = false;
            try {
                if (zK.exists(znodePath, true) != null) {
                    watched = true;
                }
            } catch (KeeperException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return watched;
        }
        public List<String> getZNodeChildren(String znodePath) {
            List<String> childNodes = null;
            try {
                childNodes = zK.getChildren(znodePath, false);
            } catch (KeeperException | InterruptedException e) {
                throw new IllegalStateException(e);
            }

            return childNodes;
        }
        public long getZNodeCreationTime(String znodePath) {
            try {
                return zK.exists(znodePath, false).getCtime();
            } catch (KeeperException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        protected void cleanShutdown(){
             try {
                 logger.info("Shutting Down Election at: " + DEFAULT_ZNODE);
                zK.close();
            } catch (InterruptedException e) {
                logger.error("Interrupt Occured While Shutting Down " + e.getMessage());
            }finally {
                zK = null;
                Thread.currentThread().interrupt();
            }
        }
    }// End ElectionZookeeper Class
    public class ElectorWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            final EventType eventType = event.getType();
            final KeeperState keeperState = event.getState();
            if (EventType.NodeDeleted.equals(eventType)) {
                try {
                if (event.getPath().equalsIgnoreCase(watchedElector)) {
                    //The guy we are following in line to vote died, rejoin
                    joinElection();
                    } else if (event.getPath().equalsIgnoreCase(electorID)){
                        //We died... Rejoin, try to delete our old node just in case it still exists
                        logger.warn(electorID + " Lost Leader Status for election at: " + DEFAULT_ZNODE);
                        isLeader = false;
                        electorID = ezk.createZNode(DEFAULT_ZNODE + ZK_SEQ, true);
                        joinElection();
                }
                } catch (Exception e) {
                     logger.debug("Somthing Bad Just Occured in the Watcher Catchall - removing our own node if possible " + e.getMessage());
                     ezk.removeZNode(electorID);
                     throw new IllegalStateException(e);
                }
            }else if(KeeperState.Expired.equals(keeperState) || KeeperState.Disconnected.equals(keeperState) ){
            	logger.info("Expired KeeperState - Attempting renew");
            }
        }
    } // End FollowerWatcher Class
}
