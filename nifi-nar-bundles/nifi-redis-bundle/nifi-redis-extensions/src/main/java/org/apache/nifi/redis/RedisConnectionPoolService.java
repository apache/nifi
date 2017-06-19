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
package org.apache.nifi.redis;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({ "redis", "cache" })
@CapabilityDescription("A service that provides connections to Redis.")
public class RedisConnectionPoolService extends AbstractControllerService implements RedisConnectionPool {

    static final AllowableValue REDIS_MODE_STANDALONE = new AllowableValue(RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDescription());
    static final AllowableValue REDIS_MODE_SENTINEL = new AllowableValue(RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDescription());
    static final AllowableValue REDIS_MODE_CLUSTER = new AllowableValue(RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDescription());

    public static final PropertyDescriptor REDIS_MODE = new PropertyDescriptor.Builder()
            .name("redis-mode")
            .displayName("Redis Mode")
            .description("The type of Redis being communicated with - standalone, sentinel, or clustered.")
            .allowableValues(REDIS_MODE_STANDALONE, REDIS_MODE_SENTINEL, REDIS_MODE_CLUSTER)
            .defaultValue(REDIS_MODE_STANDALONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("connection-string")
            .displayName("Connection String")
            .description("The connection string for Redis. In a standalone instance this value will be of the form hostname:port. " +
                    "In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. " +
                    "In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database-index")
            .displayName("Database Index")
            .description("The database index to be used by connections created from this connection pool. " +
                    "See the databases property in redis.conf, by default databases 0-15 will be available.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final PropertyDescriptor COMMUNICATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("communication-timeout")
            .displayName("Communication Timeout")
            .description("The timeout to use when attempting to communicate with Redis.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor CLUSTER_MAX_REDIRECTS = new PropertyDescriptor.Builder()
            .name("cluster-max-redirects")
            .displayName("Cluster Max Redirects")
            .description("The maximum number of redirects that can be performed when clustered.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .required(true)
            .build();

    public static final PropertyDescriptor SENTINEL_MASTER = new PropertyDescriptor.Builder()
            .name("sentinel-master")
            .displayName("Sentinel Master")
            .description("The name of the sentinel master, require when Mode is set to Sentinel")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("The password used to authenticate to the Redis server. See the requirepass property in redis.conf.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_TOTAL = new PropertyDescriptor.Builder()
            .name("pool-max-total")
            .displayName("Pool - Max Total")
            .description("The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). " +
                    "A negative value indicates that there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_IDLE = new PropertyDescriptor.Builder()
            .name("pool-max-idle")
            .displayName("Pool - Max Idle")
            .description("The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_IDLE = new PropertyDescriptor.Builder()
            .name("pool-min-idle")
            .displayName("Pool - Min Idle")
            .description("The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is " +
                    "greater than the configured value for Max Idle, then the value of Max Idle will be used instead.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_BLOCK_WHEN_EXHAUSTED = new PropertyDescriptor.Builder()
            .name("pool-block-when-exhausted")
            .displayName("Pool - Block When Exhausted")
            .description("Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. " +
                    "Setting this to false means an error will occur immediately when a client requests a connection and none are available.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("pool-max-wait-time")
            .displayName("Pool - Max Wait Time")
            .description("The amount of time to wait for an available connection when Block When Exhausted is set to true.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("pool-min-evictable-idle-time")
            .displayName("Pool - Min Evictable Idle Time")
            .description("The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("60 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TIME_BETWEEN_EVICTION_RUNS = new PropertyDescriptor.Builder()
            .name("pool-time-between-eviction-runs")
            .displayName("Pool - Time Between Eviction Runs")
            .description("The amount of time between attempting to evict idle connections from the pool.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_NUM_TESTS_PER_EVICTION_RUN = new PropertyDescriptor.Builder()
            .name("pool-num-test-per-eviction-run")
            .displayName("Pool - Num Tests Per Eviction Run")
            .description("The number of connections to tests per eviction attempt. A negative value indicates to test all connections.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("-1")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_CREATE = new PropertyDescriptor.Builder()
            .name("pool-test-on-create")
            .displayName("Pool - Test On Create")
            .description("Whether or not connections should be tested upon creation.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_BORROW = new PropertyDescriptor.Builder()
            .name("pool-test-on-borrow")
            .displayName("Pool - Test On Borrow")
            .description("Whether or not connections should be tested upon borrowing from the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_RETURN = new PropertyDescriptor.Builder()
            .name("pool-test-on-return")
            .displayName("Pool - Test On Return")
            .description("Whether or not connections should be tested upon returning to the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_WHILE_IDLE = new PropertyDescriptor.Builder()
            .name("pool-test-while-idle")
            .displayName("Pool - Test While Idle")
            .description("Whether or not connections should be tested while idle.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_MODE);
        props.add(CONNECTION_STRING);
        props.add(DATABASE);
        props.add(COMMUNICATION_TIMEOUT);
        props.add(CLUSTER_MAX_REDIRECTS);
        props.add(SENTINEL_MASTER);
        props.add(PASSWORD);
        props.add(POOL_MAX_TOTAL);
        props.add(POOL_MAX_IDLE);
        props.add(POOL_MIN_IDLE);
        props.add(POOL_BLOCK_WHEN_EXHAUSTED);
        props.add(POOL_MAX_WAIT_TIME);
        props.add(POOL_MIN_EVICTABLE_IDLE_TIME);
        props.add(POOL_TIME_BETWEEN_EVICTION_RUNS);
        props.add(POOL_NUM_TESTS_PER_EVICTION_RUN);
        props.add(POOL_TEST_ON_CREATE);
        props.add(POOL_TEST_ON_BORROW);
        props.add(POOL_TEST_ON_RETURN);
        props.add(POOL_TEST_WHILE_IDLE);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(props);
    }

    private volatile ConfigurationContext context;
    private volatile RedisType redisType;
    private volatile JedisConnectionFactory connectionFactory;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String redisMode = validationContext.getProperty(REDIS_MODE).getValue();
        final String connectionString = validationContext.getProperty(CONNECTION_STRING).evaluateAttributeExpressions().getValue();
        final Integer dbIndex = validationContext.getProperty(DATABASE).evaluateAttributeExpressions().asInteger();

        if (StringUtils.isBlank(connectionString)) {
            results.add(new ValidationResult.Builder()
                    .subject(CONNECTION_STRING.getDisplayName())
                    .valid(false)
                    .explanation("Connection String cannot be blank")
                    .build());
        } else if (REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final String[] hostAndPort = connectionString.split("[:]");
            if (hostAndPort == null || hostAndPort.length != 2 || !isInteger(hostAndPort[1])) {
                results.add(new ValidationResult.Builder()
                        .subject(CONNECTION_STRING.getDisplayName())
                        .input(connectionString)
                        .valid(false)
                        .explanation("Standalone Connection String must be in the form host:port")
                        .build());
            }
        } else {
            for (final String connection : connectionString.split("[,]")) {
                final String[] hostAndPort = connection.split("[:]");
                if (hostAndPort == null || hostAndPort.length != 2 || !isInteger(hostAndPort[1])) {
                    results.add(new ValidationResult.Builder()
                            .subject(CONNECTION_STRING.getDisplayName())
                            .input(connection)
                            .valid(false)
                            .explanation("Connection String must be in the form host:port")
                            .build());
                }
            }
        }

        if (REDIS_MODE_CLUSTER.getValue().equals(redisMode) && dbIndex > 0) {
            results.add(new ValidationResult.Builder()
                    .subject(DATABASE.getDisplayName())
                    .valid(false)
                    .explanation("Database Index must be 0 when using clustered Redis")
                    .build());
        }

        if (REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String sentinelMaster = validationContext.getProperty(SENTINEL_MASTER).evaluateAttributeExpressions().getValue();
            if (StringUtils.isEmpty(sentinelMaster)) {
                results.add(new ValidationResult.Builder()
                        .subject(SENTINEL_MASTER.getDisplayName())
                        .valid(false)
                        .explanation("Sentinel Master must be provided when Mode is Sentinel")
                        .build());
            }
        }

        return results;
    }

    private boolean isInteger(final String number) {
        try {
            Integer.parseInt(number);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;

        final String redisMode = context.getProperty(REDIS_MODE).getValue();
        this.redisType = RedisType.fromDisplayName(redisMode);
    }

    @OnDisabled
    public void onDisabled() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
            redisType = null;
            context = null;
        }
    }

    @Override
    public RedisType getRedisType() {
        return redisType;
    }

    @Override
    public RedisConnection getConnection() {
        if (connectionFactory == null) {
            synchronized (this) {
                if (connectionFactory == null) {
                    connectionFactory = createConnectionFactory();
                }
            }
        }

        return connectionFactory.getConnection();
    }

    protected JedisConnectionFactory createConnectionFactory() {
        final String redisMode = context.getProperty(REDIS_MODE).getValue();
        final String connectionString = context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions().getValue();
        final Integer dbIndex = context.getProperty(DATABASE).evaluateAttributeExpressions().asInteger();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        final Integer timeout = context.getProperty(COMMUNICATION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final JedisPoolConfig poolConfig = createJedisPoolConfig(context);

        JedisConnectionFactory connectionFactory;

        if (REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final JedisShardInfo jedisShardInfo = createJedisShardInfo(connectionString, timeout, password);

            getLogger().info("Connecting to Redis in standalone mode at " + connectionString);
            connectionFactory = new JedisConnectionFactory(jedisShardInfo);

        } else if (REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String[] sentinels = connectionString.split("[,]");
            final String sentinelMaster = context.getProperty(SENTINEL_MASTER).evaluateAttributeExpressions().getValue();
            final RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration(sentinelMaster, new HashSet<>(Arrays.asList(sentinels)));
            final JedisShardInfo jedisShardInfo = createJedisShardInfo(sentinels[0], timeout, password);

            getLogger().info("Connecting to Redis in sentinel mode...");
            getLogger().info("Redis master = " + sentinelMaster);

            for (final String sentinel : sentinels) {
                getLogger().info("Redis sentinel at " + sentinel);
            }

            connectionFactory = new JedisConnectionFactory(sentinelConfiguration, poolConfig);
            connectionFactory.setShardInfo(jedisShardInfo);

        } else {
            final String[] clusterNodes = connectionString.split("[,]");
            final Integer maxRedirects = context.getProperty(CLUSTER_MAX_REDIRECTS).asInteger();

            final RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration(Arrays.asList(clusterNodes));
            clusterConfiguration.setMaxRedirects(maxRedirects);

            getLogger().info("Connecting to Redis in clustered mode...");
            for (final String clusterNode : clusterNodes) {
                getLogger().info("Redis cluster node at " + clusterNode);
            }

            connectionFactory = new JedisConnectionFactory(clusterConfiguration, poolConfig);
        }

        connectionFactory.setUsePool(true);
        connectionFactory.setPoolConfig(poolConfig);
        connectionFactory.setDatabase(dbIndex);
        connectionFactory.setTimeout(timeout);

        if (!StringUtils.isBlank(password)) {
            connectionFactory.setPassword(password);
        }

        // need to call this to initialize the pool/connections
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    protected JedisShardInfo createJedisShardInfo(final String hostAndPort, final Integer timeout, final String password) {
        final String[] hostAndPortSplit = hostAndPort.split("[:]");
        final String host = hostAndPortSplit[0].trim();
        final Integer port = Integer.parseInt(hostAndPortSplit[1].trim());

        final JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port);
        jedisShardInfo.setConnectionTimeout(timeout);
        jedisShardInfo.setSoTimeout(timeout);

        if (!StringUtils.isEmpty(password)) {
            jedisShardInfo.setPassword(password);
        }

        return jedisShardInfo;
    }

    protected JedisPoolConfig createJedisPoolConfig(final ConfigurationContext context) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(context.getProperty(POOL_MAX_TOTAL).asInteger());
        poolConfig.setMaxIdle(context.getProperty(POOL_MAX_IDLE).asInteger());
        poolConfig.setMinIdle(context.getProperty(POOL_MIN_IDLE).asInteger());
        poolConfig.setBlockWhenExhausted(context.getProperty(POOL_BLOCK_WHEN_EXHAUSTED).asBoolean());
        poolConfig.setMaxWaitMillis(context.getProperty(POOL_MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setMinEvictableIdleTimeMillis(context.getProperty(POOL_MIN_EVICTABLE_IDLE_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setTimeBetweenEvictionRunsMillis(context.getProperty(POOL_TIME_BETWEEN_EVICTION_RUNS).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setNumTestsPerEvictionRun(context.getProperty(POOL_NUM_TESTS_PER_EVICTION_RUN).asInteger());
        poolConfig.setTestOnCreate(context.getProperty(POOL_TEST_ON_CREATE).asBoolean());
        poolConfig.setTestOnBorrow(context.getProperty(POOL_TEST_ON_BORROW).asBoolean());
        poolConfig.setTestOnReturn(context.getProperty(POOL_TEST_ON_RETURN).asBoolean());
        poolConfig.setTestWhileIdle(context.getProperty(POOL_TEST_WHILE_IDLE).asBoolean());
        return poolConfig;
    }

}
