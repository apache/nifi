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
package org.apache.nifi.toolkit.zkmigrator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ZooKeeperMigrator {

    enum AuthMode {OPEN, DIGEST, SASL}

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperMigrator.class);
    private static final String SCHEME_DIGEST = AuthMode.DIGEST.name().toLowerCase();

    private final ZooKeeperEndpointConfig zooKeeperEndpointConfig;

    ZooKeeperMigrator(String zookeeperEndpoint) throws URISyntaxException {
        LOGGER.debug("ZooKeeper endpoint parameter: {}", zookeeperEndpoint);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zookeeperEndpoint), "connectString must not be null");
        final String[] connectStringPath = zookeeperEndpoint.split("/", 2);
        Preconditions.checkArgument(connectStringPath.length >= 1, "invalid ZooKeeper endpoint: %s", zookeeperEndpoint);
        final String connectString = connectStringPath[0];
        final String path;
        if (connectStringPath.length == 2) {
            path = connectStringPath[1];
        } else {
            path = "";
        }
        this.zooKeeperEndpointConfig = new ZooKeeperEndpointConfig(connectString, "/" + path);
    }

    void readZooKeeper(OutputStream zkData, AuthMode authMode, byte[] authData) throws IOException, KeeperException, InterruptedException, ExecutionException {
        ZooKeeper zooKeeper = getZooKeeper(zooKeeperEndpointConfig, authMode, authData);
        JsonWriter jsonWriter = new JsonWriter(new BufferedWriter(new OutputStreamWriter(zkData)));
        jsonWriter.setIndent("  ");
        JsonParser jsonParser = new JsonParser();
        Gson gson = new GsonBuilder().create();

        jsonWriter.beginArray();

        // persist source ZooKeeperEndpointConfig
        gson.toJson(jsonParser.parse(gson.toJson(zooKeeperEndpointConfig)).getAsJsonObject(), jsonWriter);

        LOGGER.info("Persisting data from source ZooKeeper: {}", zooKeeperEndpointConfig);
        final List<CompletableFuture<Void>> readFutures = streamPaths(getNode(zooKeeper, zooKeeperEndpointConfig.getPath()))
                .parallel()
                .map(node ->
                        CompletableFuture.supplyAsync(() -> {
                            final DataStatAclNode dataStatAclNode = retrieveNode(zooKeeper, node);
                            LOGGER.debug("retrieved node {} from {}", dataStatAclNode, zooKeeperEndpointConfig);
                            return dataStatAclNode;
                        }).thenAccept(dataStatAclNode -> {
                            // persist each zookeeper node
                            synchronized (jsonWriter) {
                                gson.toJson(jsonParser.parse(gson.toJson(dataStatAclNode)).getAsJsonObject(), jsonWriter);
                            }
                        })
                ).collect(Collectors.toList());

        CompletableFuture<Void> allReadsFuture = CompletableFuture.allOf(readFutures.toArray(new CompletableFuture[readFutures.size()]));
        final CompletableFuture<List<Void>> finishedReads = allReadsFuture
                .thenApply(v -> readFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
        final List<Void> readsDone = finishedReads.get();
        jsonWriter.endArray();
        jsonWriter.close();
        if (LOGGER.isInfoEnabled()) {
            final int readCount = readsDone.size();
            LOGGER.info("{} {} read from {}", readCount, readCount == 1 ? "node" : "nodes", zooKeeperEndpointConfig);
        }
    }

    void writeZooKeeper(InputStream zkData, AuthMode authMode, byte[] authData) throws IOException, ExecutionException, InterruptedException {
        ZooKeeper zooKeeper = getZooKeeper(zooKeeperEndpointConfig, authMode, authData);
        JsonReader jsonReader = new JsonReader(new BufferedReader(new InputStreamReader(zkData)));
        Gson gson = new GsonBuilder().create();

        jsonReader.beginArray();

        // determine source ZooKeeperEndpointConfig for this data
        final ZooKeeperEndpointConfig sourceZooKeeperEndpointConfig = gson.fromJson(jsonReader, ZooKeeperEndpointConfig.class);
        LOGGER.info("Source data was obtained from ZooKeeper: {}", sourceZooKeeperEndpointConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceZooKeeperEndpointConfig.getConnectString()) && !Strings.isNullOrEmpty(sourceZooKeeperEndpointConfig.getPath()),
                "Source ZooKeeper %s from %s is invalid", sourceZooKeeperEndpointConfig, zkData);
        Preconditions.checkState(!zooKeeperEndpointConfig.equals(sourceZooKeeperEndpointConfig),
                "Source ZooKeeper config %s for the data provided can not be the same as the configured destionation ZooKeeper config %s",
                sourceZooKeeperEndpointConfig, zooKeeperEndpointConfig);

        // stream through each node read from the json input
        final Stream<DataStatAclNode> stream = StreamSupport.stream(new Spliterators.AbstractSpliterator<DataStatAclNode>(0, 0) {
            @Override
            public boolean tryAdvance(Consumer<? super DataStatAclNode> action) {
                try {
                    // stream each DataStatAclNode from configured json file
                    synchronized (jsonReader) {
                        if (jsonReader.hasNext()) {
                            action.accept(gson.fromJson(jsonReader, DataStatAclNode.class));
                            return true;
                        } else {
                            return false;
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("unable to read nodes from json", e);
                }
            }
        }, false);

        final List<CompletableFuture<Stat>> writeFutures = stream.parallel().map(node -> {
            /*
             * create stage to migrate paths and ACLs based on the migration parent path plus the node path and the given AuthMode,
             * this stage must be run first
             */
            final CompletableFuture<DataStatAclNode> transformNodeStage = CompletableFuture.supplyAsync(() -> transformNode(node, authMode));
            /*
             * create stage to ensure that nodes exist for the entire path of the zookeeper node, must be invoked after the transformNode stage to
             * ensure that the node will exist after path migration
             */
            final Function<DataStatAclNode, String> ensureNodeExistsStage = dataStatAclNode ->
                    ensureNodeExists(zooKeeper, dataStatAclNode.getPath(), dataStatAclNode.getEphemeralOwner() == 0 ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL);
            /*
             * create stage that waits for both the transformNode and ensureNodeExists stages complete, and also provides that the given transformed node is
             * available to the next stage
             */
            final BiFunction<String, DataStatAclNode, DataStatAclNode> combineEnsureNodeAndTransferNodeStage = (u, dataStatAclNode) -> dataStatAclNode;
            /*
             * create stage to transmit the node to the destination zookeeper endpoint, must be invoked after the node has been transformed and its path
             * has been created (or already exists) in the destination zookeeper
             */
            final Function<DataStatAclNode, CompletionStage<Stat>> transmitNodeStage = dataStatNode ->
                    CompletableFuture.supplyAsync(() -> transmitNode(zooKeeper, dataStatNode));
            /*
             * submit the stages chained together in the proper order to perform the processing on the given node
             */
            return transformNodeStage.thenApply(ensureNodeExistsStage).thenCombine(transformNodeStage, combineEnsureNodeAndTransferNodeStage).thenCompose(transmitNodeStage);
        }).collect(Collectors.toList());

        CompletableFuture<Void> allWritesFuture = CompletableFuture.allOf(writeFutures.toArray(new CompletableFuture[writeFutures.size()]));
        final CompletableFuture<List<Stat>> finishedWrites = allWritesFuture
                .thenApply(v -> writeFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
        final List<Stat> writesDone = finishedWrites.get();
        if (LOGGER.isInfoEnabled()) {
            final int writeCount = writesDone.size();
            LOGGER.info("{} {} transferred to {}", writeCount, writeCount == 1 ? "node" : "nodes", zooKeeperEndpointConfig);
        }
        jsonReader.close();
    }

    private Stream<String> streamPaths(ZooKeeperNode node) {
        return Stream.concat(Stream.of(node.getPath()), node.getChildren().stream().flatMap(this::streamPaths));
    }

    private ZooKeeperNode getNode(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        LOGGER.debug("retrieving node and children at {}", path);
        final List<String> children = zooKeeper.getChildren(path, false);
        return new ZooKeeperNode(path, children.stream().map(s -> {
            final String childPath = Joiner.on('/').skipNulls().join(path.equals("/") ? "" : path, s);
            try {
                return getNode(zooKeeper, childPath);
            } catch (Exception e) {
                throw new RuntimeException(String.format("unable to discover sub-tree from %s", childPath), e);
            }
        }).collect(Collectors.toList()));
    }

    private DataStatAclNode retrieveNode(ZooKeeper zooKeeper, String path) {
        Preconditions.checkNotNull(zooKeeper, "ZooKeeper client must not be null");
        Preconditions.checkNotNull(path, "path must not be null");
        final Stat stat = new Stat();
        final byte[] data;
        final List<ACL> acls;
        final long ephemeralOwner;
        try {
            data = zooKeeper.getData(path, false, stat);
            acls = zooKeeper.getACL(path, stat);
            ephemeralOwner = stat.getEphemeralOwner();
        } catch (Exception e) {
            throw new RuntimeException(String.format("unable to get data, ACLs, and stats from %s for node at path %s", zooKeeper, path), e);
        }
        return new DataStatAclNode(path, data, stat, acls, ephemeralOwner);
    }

    private String ensureNodeExists(ZooKeeper zooKeeper, String path, CreateMode createMode) {
        try {
            LOGGER.debug("attempting to create node at {}", path);
            final ArrayList<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
            final String createNodePath = zooKeeper.create(path, new byte[0], acls, createMode);
            LOGGER.info("created node at {}, acls: {}, createMode: {}", createNodePath, acls, createMode);
            return createNodePath;
        } catch (KeeperException e) {
            if (KeeperException.Code.NONODE.equals(e.code())) {
                final List<String> pathTokens = Splitter.on('/').omitEmptyStrings().trimResults().splitToList(path);
                final String parentPath = "/" + Joiner.on('/').skipNulls().join(pathTokens.subList(0, pathTokens.size() - 1));
                LOGGER.debug("node doesn't exist, recursively attempting to create node at {}", parentPath);
                ensureNodeExists(zooKeeper, parentPath, CreateMode.PERSISTENT);
                LOGGER.debug("recursively created node at {}", parentPath);
                LOGGER.debug("retrying attempt to create node at {}", path);
                return ensureNodeExists(zooKeeper, path, createMode);
            } else if (KeeperException.Code.NODEEXISTS.equals(e.code())) {
                return path;
            } else {
                throw new RuntimeException(String.format("unable to create node at path %s, ZooKeeper returned %s", path, e.code()), e);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("unable to create node at path %s", path), e);
        }
    }

    private DataStatAclNode transformNode(DataStatAclNode node, AuthMode destinationAuthMode) {
        String migrationPath = '/' + Joiner.on('/').skipNulls().join(Splitter.on('/').omitEmptyStrings().trimResults().split(zooKeeperEndpointConfig.getPath() + node.getPath()));
        // For the NiFi use case, all nodes will be migrated to CREATOR_ALL_ACL
        final DataStatAclNode migratedNode = new DataStatAclNode(migrationPath, node.getData(), node.getStat(),
                destinationAuthMode.equals(AuthMode.OPEN) ? ZooDefs.Ids.OPEN_ACL_UNSAFE : ZooDefs.Ids.CREATOR_ALL_ACL,
                node.getEphemeralOwner());
        LOGGER.info("transformed original node {} to {}", node, migratedNode);
        return migratedNode;
    }

    private Stat transmitNode(ZooKeeper zooKeeper, DataStatAclNode node) {
        Preconditions.checkNotNull(zooKeeper, "zooKeeper must not be null");
        Preconditions.checkNotNull(node, "node must not be null");
        try {
            LOGGER.debug("attempting to transfer node to {} with ACL {}: {}", zooKeeperEndpointConfig, node.getAcls(), node);
            // set data without caring what the previous version of the data at that path
            zooKeeper.setData(node.getPath(), node.getData(), -1);
            zooKeeper.setACL(node.getPath(), node.getAcls(), -1);
            LOGGER.info("transfered node {} in {}", node, zooKeeperEndpointConfig);
        } catch (Exception e) {
            throw new RuntimeException(String.format("unable to transmit data to %s for path %s", zooKeeper, node.getPath()), e);
        }
        return node.getStat();
    }

    private ZooKeeper getZooKeeper(ZooKeeperEndpointConfig zooKeeperEndpointConfig, AuthMode authMode, byte[] authData) throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper(zooKeeperEndpointConfig.getConnectString(), 3000, watchedEvent -> {
        });
        if (authMode.equals(AuthMode.DIGEST)) {
            zooKeeper.addAuthInfo(SCHEME_DIGEST, authData);
        }
        return zooKeeper;
    }

    ZooKeeperEndpointConfig getZooKeeperEndpointConfig() {
        return zooKeeperEndpointConfig;
    }
}
