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
package org.apache.nifi.cluster.flow.impl;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.cluster.flow.ClusterDataFlow;
import org.apache.nifi.cluster.flow.DaoException;
import org.apache.nifi.cluster.flow.DataFlowDao;
import org.apache.nifi.cluster.flow.DataFlowManagementService;
import org.apache.nifi.cluster.flow.PersistedFlowState;
import org.apache.nifi.cluster.protocol.ClusterManagerProtocolSender;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements FlowManagementService interface. The service tries to keep the
 * cluster's flow current with regards to the available nodes.
 *
 * The instance may be configured with a retrieval delay, which will reduce the
 * number of retrievals performed by the service at the expense of increasing
 * the chances that the service will not be able to provide a current flow to
 * the caller.
 *
 * By default, the service will try to update the flow as quickly as possible.
 * Configuring a delay enables a less aggressive retrieval strategy.
 * Specifically, the eligible retrieval time is reset every time the flow state
 * is set to STALE. If the state is set to UNKNOWN or CURRENT, then the flow
 * will not be retrieved.
 *
 * @author unattributed
 */
public class DataFlowManagementServiceImpl implements DataFlowManagementService {

    /*
     * Developer Note: 
     * 
     * This class maintains an ExecutorService and a Runnable.
     * Although the class is not externally threadsafe, its internals are protected to
     * accommodate multithread access between the ExecutorServer and the Runnable.
     * 
     */
    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(DataFlowManagementServiceImpl.class));

    private final DataFlowDao flowDao;

    private final ClusterManagerProtocolSender sender;

    private final Set<NodeIdentifier> nodeIds = new CopyOnWriteArraySet<>();

    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private final AtomicLong lastRetrievalTime = new AtomicLong(-1);

    private Timer flowRetriever;

    private long retrievableAfterTime = 0L;

    private AtomicInteger retrievalDelaySeconds = new AtomicInteger(0);

    private final TimingReentrantLock resourceLock = new TimingReentrantLock(new ReentrantLock());

    public DataFlowManagementServiceImpl(final DataFlowDao flowDao, final ClusterManagerProtocolSender sender) {
        if (flowDao == null) {
            throw new IllegalArgumentException("Flow DAO may not be null.");
        } else if (sender == null) {
            throw new IllegalArgumentException("Cluster Manager Protocol Sender may not be null.");
        }
        this.flowDao = flowDao;
        this.sender = sender;
    }

    @Override
    public void start() {

        if (isRunning()) {
            throw new IllegalArgumentException("Instance is already running.");
        }

        // reset stop requested
        stopRequested.set(false);

        // setup flow retreiver timer
        flowRetriever = new Timer("Flow Management Service", /* is daemon */ true);
        flowRetriever.schedule(new FlowRetrieverTimerTask(), 0, 500);
    }

    @Override
    public boolean isRunning() {
        return (flowRetriever != null);
    }

    @Override
    public void stop() {

        if (isRunning() == false) {
            throw new IllegalArgumentException("Instance is already stopped.");
        }

        // record stop request
        stopRequested.set(true);

        flowRetriever.cancel();
        flowRetriever = null;

    }

    @Override
    public ClusterDataFlow loadDataFlow() throws DaoException {
        resourceLock.lock();
        try {
            return flowDao.loadDataFlow();
        } finally {
            resourceLock.unlock("loadDataFlow");
        }
    }

    @Override
    public void updatePrimaryNode(final NodeIdentifier nodeId) {
        resourceLock.lock();
        try {
            final ClusterDataFlow existingClusterDataFlow = flowDao.loadDataFlow();

            final StandardDataFlow dataFlow;
            final byte[] controllerServiceBytes;
            final byte[] reportingTaskBytes;
            if (existingClusterDataFlow == null) {
                dataFlow = null;
                controllerServiceBytes = new byte[0];
                reportingTaskBytes = new byte[0];
            } else {
                dataFlow = existingClusterDataFlow.getDataFlow();
                controllerServiceBytes = existingClusterDataFlow.getControllerServices();
                reportingTaskBytes = existingClusterDataFlow.getReportingTasks();
            }

            flowDao.saveDataFlow(new ClusterDataFlow(dataFlow, nodeId, controllerServiceBytes, reportingTaskBytes));
        } finally {
            resourceLock.unlock("updatePrimaryNode");
        }
    }
    
    
    @Override
    public void updateControllerServices(final byte[] controllerServiceBytes) throws DaoException {
    	resourceLock.lock();
    	try {
    		final ClusterDataFlow existingClusterDataFlow = flowDao.loadDataFlow();

            final StandardDataFlow dataFlow;
            final byte[] reportingTaskBytes;
            final NodeIdentifier nodeId;
            if (existingClusterDataFlow == null) {
                dataFlow = null;
                nodeId = null;
                reportingTaskBytes = new byte[0];
            } else {
                dataFlow = existingClusterDataFlow.getDataFlow();
                nodeId = existingClusterDataFlow.getPrimaryNodeId();
                reportingTaskBytes = existingClusterDataFlow.getReportingTasks();
            }

            flowDao.saveDataFlow(new ClusterDataFlow(dataFlow, nodeId, controllerServiceBytes, reportingTaskBytes));
    	} finally {
    		resourceLock.unlock("updateControllerServices");
    	}
    }
    
    @Override
    public void updateReportingTasks(final byte[] reportingTaskBytes) throws DaoException {
    	resourceLock.lock();
    	try {
    		final ClusterDataFlow existingClusterDataFlow = flowDao.loadDataFlow();

            final StandardDataFlow dataFlow;
            final byte[] controllerServiceBytes;
            final NodeIdentifier nodeId;
            if (existingClusterDataFlow == null) {
                dataFlow = null;
                nodeId = null;
                controllerServiceBytes = null;
            } else {
                dataFlow = existingClusterDataFlow.getDataFlow();
                nodeId = existingClusterDataFlow.getPrimaryNodeId();
                controllerServiceBytes = existingClusterDataFlow.getControllerServices();
            }

            flowDao.saveDataFlow(new ClusterDataFlow(dataFlow, nodeId, controllerServiceBytes, reportingTaskBytes));
    	} finally {
    		resourceLock.unlock("updateControllerServices");
    	}
    }

    @Override
    public PersistedFlowState getPersistedFlowState() {
        resourceLock.lock();
        try {
            return flowDao.getPersistedFlowState();
        } finally {
            resourceLock.unlock("getPersistedFlowState");
        }
    }

    @Override
    public boolean isFlowCurrent() {
        return PersistedFlowState.CURRENT == getPersistedFlowState();
    }

    @Override
    public void setPersistedFlowState(final PersistedFlowState flowState) {
        // lock to ensure state change and retrievable time update are atomic
        resourceLock.lock();
        try {
            flowDao.setPersistedFlowState(flowState);
            if (PersistedFlowState.STALE == flowState) {
                retrievableAfterTime = new Date().getTime() + (getRetrievalDelaySeconds() * 1000);
            } else if (PersistedFlowState.UNKNOWN == flowState || PersistedFlowState.CURRENT == flowState) {
                retrievableAfterTime = Long.MAX_VALUE;
            }
        } finally {
            resourceLock.unlock("setPersistedFlowState");
        }
    }

    @Override
    public Set<NodeIdentifier> getNodeIds() {
        return Collections.unmodifiableSet(nodeIds);
    }

    @Override
    public void setNodeIds(final Set<NodeIdentifier> nodeIds) {

        if (nodeIds == null) {
            throw new IllegalArgumentException("Node IDs may not be null.");
        }

        resourceLock.lock();
        try {

            if (this.nodeIds.equals(nodeIds)) {
                return;
            }

            this.nodeIds.clear();
            this.nodeIds.addAll(nodeIds);

        } finally {
            resourceLock.unlock("setNodeIds");
        }

    }

    @Override
    public int getRetrievalDelaySeconds() {
        return retrievalDelaySeconds.get();
    }

    @Override
    public void setRetrievalDelay(final String retrievalDelay) {
        this.retrievalDelaySeconds.set((int) FormatUtils.getTimeDuration(retrievalDelay, TimeUnit.SECONDS));
    }

    public ClusterManagerProtocolSender getSender() {
        return sender;
    }

    public long getLastRetrievalTime() {
        return lastRetrievalTime.get();
    }

    /**
     * A timer task for issuing FlowRequestMessage messages to nodes to retrieve
     * an updated flow.
     */
    private class FlowRetrieverTimerTask extends TimerTask {

        @Override
        public void run() {

            resourceLock.lock();
            try {
                // if flow is current, then we're done
                if (isFlowCurrent()) {
                    return;
                }
            } catch (final Exception ex) {
                logger.info("Encountered exception checking if flow is current caused by " + ex, ex);
            } finally {
                resourceLock.unlock("FlowRetrieverTimerTask - isFlowCurrent");
            }

            final FlowRequestMessage request = new FlowRequestMessage();
            for (final NodeIdentifier nodeId : getNodeIds()) {
                try {
                    // setup request
                    request.setNodeId(nodeId);

                    // record request time
                    final long requestSentTime = new Date().getTime();

                    resourceLock.lock();
                    try {
                        // sanity checks before making request
                        if (stopRequested.get()) {  // did we receive a stop request
                            logger.debug("Stopping runnable prematurely because a request to stop was issued.");
                            return;
                        } else if (requestSentTime < retrievableAfterTime) {
                            /*
                             * Retrievable after time was updated while obtaining
                             * the lock, so try again later
                             */
                            return;
                        }
                    } finally {
                        resourceLock.unlock("FlowRetrieverTimerTask - check stopRequested");
                    }

                    // send request
                    final FlowResponseMessage response = sender.requestFlow(request);

                    resourceLock.lock();
                    try {
                        // check if the retrieved flow is still valid
                        if (requestSentTime > retrievableAfterTime) {
                            logger.info("Saving retrieved flow.");

                            final StandardDataFlow dataFlow = response.getDataFlow();
                            final ClusterDataFlow existingClusterDataFlow = flowDao.loadDataFlow();
                            final ClusterDataFlow currentClusterDataFlow;
                            if (existingClusterDataFlow == null) {
                                currentClusterDataFlow = new ClusterDataFlow(dataFlow, null, new byte[0], new byte[0]);
                            } else {
                                currentClusterDataFlow = new ClusterDataFlow(dataFlow, existingClusterDataFlow.getPrimaryNodeId(), 
                                		existingClusterDataFlow.getControllerServices(), existingClusterDataFlow.getReportingTasks());
                            }
                            flowDao.saveDataFlow(currentClusterDataFlow);
                            flowDao.setPersistedFlowState(PersistedFlowState.CURRENT);
                            lastRetrievalTime.set(new Date().getTime());
                        }

                        /*
                         * Retrievable after time was updated while requesting
                         * the flow, so try again later.
                         */
                    } finally {
                        resourceLock.unlock("FlowRetrieverTimerTask - saveDataFlow");
                    }

                } catch (final Throwable t) {
                    logger.info("Encountered exception retrieving flow from node " + nodeId + " caused by " + t, t);
                }
            }
        }
    }

    private static class TimingReentrantLock {

        private final Lock lock;
        private static final Logger logger = LoggerFactory.getLogger("dataFlowManagementService.lock");

        private final ThreadLocal<Long> lockTime = new ThreadLocal<>();

        public TimingReentrantLock(final Lock lock) {
            this.lock = lock;
        }

        public void lock() {
            lock.lock();
            lockTime.set(System.nanoTime());
        }

        public void unlock(final String task) {
            final long nanosLocked = System.nanoTime() - lockTime.get();
            lock.unlock();

            final long millisLocked = TimeUnit.MILLISECONDS.convert(nanosLocked, TimeUnit.NANOSECONDS);
            if (millisLocked > 100L) {
                logger.debug("Lock held for {} milliseconds for task: {}", millisLocked, task);
            }
        }
    }
}
