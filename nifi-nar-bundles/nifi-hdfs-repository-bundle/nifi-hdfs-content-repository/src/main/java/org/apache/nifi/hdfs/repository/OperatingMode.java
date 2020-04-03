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
package org.apache.nifi.hdfs.repository;

/**
 * Defines the ways in which the HdfsContentRepository will operate
 */
public enum OperatingMode {

    /**
     * No special fallback handling is made during failure. Each configured
     * container is written to as normal until they are full, once all containers are
     * full, writes will block until space becomes avaialble.
     * Note: this is default operating mode if one isn't specified in the nifi.properties file.
     */
    Normal,

    /**
     * The containers in the 'primary' group are filled first, and the rest
     * are only filled once all containers in the primary group are full. Once
     * space becomes available again for at least a minute, the primary group will
     * become active again.
     *
     * Note: the 'primary' group is specified with the following property where
     * each container id is comma separated: 'nifi.content.repository.hdfs.primary'
     *
     * Example:
     * nifi.content.repository.hdfs.primary=disk1,disk2,disk
     *
     * Note: this mode cannot be used with the 'FailureFallback' mode.
     */
    CapacityFallback,

    /**
     * The configured containers 'primary' group are filled as normal until they are full.
     * Once they are full, writes will block until space becomes available.
     *
     * If a write failure ocurrs within all primary containers, the remaining non-primary
     * containers written to until a configured time period has elapsed.
     *
     * Note: see the 'CapacityFallback' mode for details on how to specify the primary group.
     *
     * Note: this mode cannot be used with the 'CapacityFallback' mode.
     */
    FailureFallback,

    /**
     * All containers are written to and filled as described in the other modes.
     * As files are moved to the archive, they are copied to the locations in the 'archive' group and then deleted.
     *
     * Note: the 'archive' group is specified with the following property where
     * each container id is comma separated: 'nifi.content.repository.hdfs.archive'
     *
     * Example:
     * nifi.content.repository.hdfs.archive=disk1,disk2,disk
     *
     * Note: this can be combined with any of the other three modes.
     * If this the only mode specified, 'Normal' is also assumed.
     */
    Archive

}
