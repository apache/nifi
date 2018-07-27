package org.apache.nifi.controller.queue.clustered.server;


import org.apache.nifi.connectable.Connection;

import java.util.Collection;

public interface LoadBalanceAuthorizer {
    void authorize(Collection<String> clientIdentities, Connection connection) throws NotAuthorizedException;
}
