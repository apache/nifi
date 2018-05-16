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
import org.apache.zookeeper.Watcher;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

    ZooKeeperMigrator(String zooKeeperConnectString) {
        LOGGER.debug("ZooKeeper connect string parameter: {}", zooKeeperConnectString);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zooKeeperConnectString), "ZooKeeper connect string must not be null");
        this.zooKeeperEndpointConfig = new ZooKeeperEndpointConfig(zooKeeperConnectString);
    }

    void readZooKeeper(OutputStream zkData, AuthMode authMode, byte[] authData) throws IOException, KeeperException, InterruptedException, ExecutionException {
        ZooKeeper zooKeeper = getZooKeeper(zooKeeperEndpointConfig.getConnectString(), authMode, authData);
        JsonWriter jsonWriter = new JsonWriter(new BufferedWriter(new OutputStreamWriter(zkData)));
        jsonWriter.setIndent("  ");
        JsonParser jsonParser = new JsonParser();
        Gson gson = new GsonBuilder().create();

        jsonWriter.beginArray();

        // persist source ZooKeeperEndpointConfig
        gson.toJson(jsonParser.parse(gson.toJson(zooKeeperEndpointConfig)).getAsJsonObject(), jsonWriter);

        LOGGER.info("Retrieving data from source ZooKeeper: {}", zooKeeperEndpointConfig);
        final List<CompletableFuture<Void>> readFutures = streamPaths(getNode(zooKeeper, "/"))
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
        closeZooKeeper(zooKeeper);
    }

    void writeZooKeeper(InputStream zkData, AuthMode authMode, byte[] authData, boolean ignoreSource, boolean useExistingACL) throws IOException, ExecutionException, InterruptedException {
        // ensure that the chroot path exists
        ZooKeeper zooKeeperRoot = getZooKeeper(Joiner.on(',').join(zooKeeperEndpointConfig.getServers()), authMode, authData);
        ensureNodeExists(zooKeeperRoot, zooKeeperEndpointConfig.getPath(), CreateMode.PERSISTENT);
        closeZooKeeper(zooKeeperRoot);

        ZooKeeper zooKeeper = getZooKeeper(zooKeeperEndpointConfig.getConnectString(), authMode, authData);
        JsonReader jsonReader = new JsonReader(new BufferedReader(new InputStreamReader(zkData)));
        Gson gson = new GsonBuilder().create();

        jsonReader.beginArray();

        // determine source ZooKeeperEndpointConfig for this data
        final ZooKeeperEndpointConfig sourceZooKeeperEndpointConfig = gson.fromJson(jsonReader, ZooKeeperEndpointConfig.class);
        LOGGER.info("Source data was obtained from ZooKeeper: {}", sourceZooKeeperEndpointConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceZooKeeperEndpointConfig.getConnectString()) && !Strings.isNullOrEmpty(sourceZooKeeperEndpointConfig.getPath())
                        && sourceZooKeeperEndpointConfig.getServers() != null && sourceZooKeeperEndpointConfig.getServers().size() > 0, "Source ZooKeeper %s from %s is invalid",
                sourceZooKeeperEndpointConfig, zkData);
        Preconditions.checkArgument(Collections.disjoint(zooKeeperEndpointConfig.getServers(), sourceZooKeeperEndpointConfig.getServers())
                        || !zooKeeperEndpointConfig.getPath().equals(sourceZooKeeperEndpointConfig.getPath()) || ignoreSource,
                "Source ZooKeeper config %s for the data provided can not contain the same server and path as the configured destination ZooKeeper config %s",
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
             * create stage to determine the acls that should be applied to the node.
             * this stage will be used to initialize the chain
             */
            final CompletableFuture<List<ACL>> determineACLStage = CompletableFuture.supplyAsync(() -> determineACLs(node, authMode, useExistingACL));
            /*
             * create stage to apply acls to nodes and transform node to DataStatAclNode object
             */
            final Function<List<ACL>, CompletableFuture<DataStatAclNode>> transformNodeStage = acls -> CompletableFuture.supplyAsync(() -> transformNode(node, acls));
            /*
             * create stage to ensure that nodes exist for the entire path of the zookeeper node, must be invoked after the transformNode stage to
             * ensure that the node will exist after path migration
             */
            final Function<DataStatAclNode, CompletionStage<String>> ensureNodeExistsStage = dataStatAclNode ->
                    CompletableFuture.supplyAsync(() -> ensureNodeExists(zooKeeper, dataStatAclNode.getPath(),
                            dataStatAclNode.getEphemeralOwner() == 0 ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL));
            /*
             * create stage that waits for both the transformNode and ensureNodeExists stages complete, and also provides that the given transformed node is
             * available to the next stage
             */
            final BiFunction<String, DataStatAclNode, DataStatAclNode> combineEnsureNodeAndTransferNodeStage = (u, dataStatAclNode) -> dataStatAclNode;
            /*
             * create stage to transmit the node to the destination zookeeper endpoint, must be invoked after the node has been transformed and its path
             * has been created (or already exists) in the destination zookeeper
             */
            final Function<DataStatAclNode, CompletionStage<Stat>> transmitNodeStage = dataStatNode -> CompletableFuture.supplyAsync(() -> transmitNode(zooKeeper, dataStatNode));
            /*
             * submit the stages chained together in the proper order to perform the processing on the given node
             */
            final CompletableFuture<DataStatAclNode> dataStatAclNodeCompletableFuture = determineACLStage.thenCompose(transformNodeStage);
            return dataStatAclNodeCompletableFuture.thenCompose(ensureNodeExistsStage)
                    .thenCombine(dataStatAclNodeCompletableFuture, combineEnsureNodeAndTransferNodeStage)
                    .thenCompose(transmitNodeStage);
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
        closeZooKeeper(zooKeeper);
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
            } catch (InterruptedException | KeeperException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
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
        } catch (InterruptedException | KeeperException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
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
            Thread.currentThread().interrupt();
            throw new RuntimeException(String.format("unable to create node at path %s", path), e);
        }
    }

    private List<ACL> determineACLs(DataStatAclNode node, AuthMode authMode, Boolean useExistingACL) {
        return useExistingACL ? node.getAcls() :
                (authMode.equals(AuthMode.OPEN) ? ZooDefs.Ids.OPEN_ACL_UNSAFE : ZooDefs.Ids.CREATOR_ALL_ACL);
    }

    private DataStatAclNode transformNode(DataStatAclNode node, List<ACL> acls) {
        final DataStatAclNode migratedNode = new DataStatAclNode(node.getPath(), node.getData(), node.getStat(), acls, node.getEphemeralOwner());
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
            LOGGER.info("transferred node {} in {}", node, zooKeeperEndpointConfig);
        } catch (InterruptedException | KeeperException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(String.format("unable to transmit data to %s for path %s", zooKeeper, node.getPath()), e);
        }
        return node.getStat();
    }

    private ZooKeeper getZooKeeper(String zooKeeperConnectString, AuthMode authMode, byte[] authData) throws IOException {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(zooKeeperConnectString, 3000, watchedEvent -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ZooKeeper server state changed to {} in {}", watchedEvent.getState(), zooKeeperConnectString);
            }
            if (watchedEvent.getType().equals(Watcher.Event.EventType.None) && watchedEvent.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                connectionLatch.countDown();
            }
        });

        final boolean connected;
        try {
            connected = connectionLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            closeZooKeeper(zooKeeper);
            Thread.currentThread().interrupt();
            throw new IOException(String.format("interrupted while waiting for ZooKeeper connection to %s", zooKeeperConnectString), e);
        }

        if (!connected) {
            closeZooKeeper(zooKeeper);
            throw new IOException(String.format("unable to connect to %s", zooKeeperConnectString));
        }

        if (authMode.equals(AuthMode.DIGEST)) {
            zooKeeper.addAuthInfo(SCHEME_DIGEST, authData);
        }
        return zooKeeper;
    }

    private void closeZooKeeper(ZooKeeper zooKeeper) {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            LOGGER.warn("could not close ZooKeeper client due to interrupt", e);
            Thread.currentThread().interrupt();
        }
    }

    ZooKeeperEndpointConfig getZooKeeperEndpointConfig() {
        return zooKeeperEndpointConfig;
    }
}
