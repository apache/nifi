package org.apache.nifi.controller.queue.clustered.server;


import java.util.Collection;

public interface LoadBalanceAuthorizer {
    void authorize(Collection<String> clientIdentities) throws NotAuthorizedException;
}
