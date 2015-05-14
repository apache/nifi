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
package org.apache.nifi.cluster.flow;

/**
 * A data access object for loading and saving the flow managed by the cluster.
 *
 */
public interface DataFlowDao {

    /**
     * Loads the cluster's dataflow.
     *
     * @return the dataflow or null if no dataflow exists
     *
     * @throws DaoException if the dataflow was unable to be loaded
     */
    ClusterDataFlow loadDataFlow() throws DaoException;

    /**
     * Saves the cluster's dataflow.
     *
     *
     * @param dataFlow flow
     * @throws DaoException if the dataflow was unable to be saved
     */
    void saveDataFlow(ClusterDataFlow dataFlow) throws DaoException;

    /**
     * Sets the state of the dataflow. If the dataflow does not exist, then an exception is thrown.
     *
     * @param flowState the state of the dataflow
     *
     * @throws DaoException if the state was unable to be updated
     */
    void setPersistedFlowState(PersistedFlowState flowState) throws DaoException;

    /**
     * Gets the state of the dataflow.
     *
     * @return the state of the dataflow
     *
     * @throws DaoException if the state was unable to be retrieved
     */
    PersistedFlowState getPersistedFlowState() throws DaoException;
}
