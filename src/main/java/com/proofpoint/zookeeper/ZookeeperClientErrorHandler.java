package com.proofpoint.zookeeper;

public interface ZookeeperClientErrorHandler
{
    /**
     * Called when a connection to ZK is lost and all retries have been exhausted
     *
     * @param client the client
     */
    public void connectionLost(ZookeeperClient client);
}
