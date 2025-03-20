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
package org.apache.nifi.redis.util;

import org.apache.nifi.redis.RedisType;

import java.time.Duration;
import java.util.Objects;

public class RedisConfig {

    private final RedisType redisMode;
    private final String connectionString;

    private String sentinelMaster;
    private String sentinelUsername;
    private String sentinelPassword;

    private String username;
    private String password;

    private int dbIndex = 0;
    private int timeout = 10000;
    private int clusterMaxRedirects = 5;

    private int poolMaxTotal = 8;
    private int poolMaxIdle = 8;
    private int poolMinIdle = 0;
    private boolean blockWhenExhausted = true;
    private Duration maxWaitTime = Duration.ofSeconds(10);
    private Duration minEvictableIdleDuration = Duration.ofSeconds(60);
    private Duration timeBetweenEvictionRuns = Duration.ofSeconds(30);
    private int numTestsPerEvictionRun = -1;
    private boolean testOnCreate = true;
    private boolean testOnBorrow = true;
    private boolean testOnReturn = false;
    private boolean testWhenIdle = true;

    public RedisConfig(final RedisType redisMode, final String connectionString) {
        this.redisMode = Objects.requireNonNull(redisMode);
        this.connectionString = Objects.requireNonNull(connectionString);
    }

    public RedisType getRedisMode() {
        return redisMode;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getSentinelMaster() {
        return sentinelMaster;
    }

    public void setSentinelMaster(String sentinelMaster) {
        this.sentinelMaster = sentinelMaster;
    }

    public String getSentinelUsername() {
        return sentinelUsername;
    }

    public void setSentinelUsername(String sentinelUsername) {
        this.sentinelUsername = sentinelUsername;
    }

    public String getSentinelPassword() {
        return sentinelPassword;
    }

    public void setSentinelPassword(String sentinelPassword) {
        this.sentinelPassword = sentinelPassword;
    }

    public int getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getClusterMaxRedirects() {
        return clusterMaxRedirects;
    }

    public void setClusterMaxRedirects(int clusterMaxRedirects) {
        this.clusterMaxRedirects = clusterMaxRedirects;
    }

    public int getPoolMaxTotal() {
        return poolMaxTotal;
    }

    public void setPoolMaxTotal(int poolMaxTotal) {
        this.poolMaxTotal = poolMaxTotal;
    }

    public int getPoolMaxIdle() {
        return poolMaxIdle;
    }

    public void setPoolMaxIdle(int poolMaxIdle) {
        this.poolMaxIdle = poolMaxIdle;
    }

    public int getPoolMinIdle() {
        return poolMinIdle;
    }

    public void setPoolMinIdle(int poolMinIdle) {
        this.poolMinIdle = poolMinIdle;
    }

    public boolean getBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    public Duration getMaxWaitTime() {
        return maxWaitTime;
    }

    public void setMaxWaitTime(Duration maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public Duration getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    public void setMinEvictableIdleDuration(Duration minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
    }

    public Duration getTimeBetweenEvictionRuns() {
        return timeBetweenEvictionRuns;
    }

    public void setTimeBetweenEvictionRuns(Duration timeBetweenEvictionRuns) {
        this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public boolean getTestOnCreate() {
        return testOnCreate;
    }

    public void setTestOnCreate(boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    public boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean getTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public boolean getTestWhenIdle() {
        return testWhenIdle;
    }

    public void setTestWhenIdle(boolean testWhenIdle) {
        this.testWhenIdle = testWhenIdle;
    }
}
