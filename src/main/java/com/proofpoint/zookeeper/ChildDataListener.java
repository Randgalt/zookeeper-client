package com.proofpoint.zookeeper;

/**
 * Listener for events on a ZK path
 */
public interface ChildDataListener
{
    /**
     * A node was added
     *
     * @param child name of the node
     * @param data data of the node
     * @throws Exception any errors
     */
    void added(String child, byte[] data)
            throws Exception;

    /**
     * A node was changed
     *
     * @param child name of the node
     * @param data data of the node
     * @param version the node version
     * @throws Exception any errors
     */
    void updated(String child, byte[] data, int version)
            throws Exception;

    /**
     * A node was removed
     *
     * @param child name of the node
     * @throws Exception any errors
     */
    void removed(String child)
            throws Exception;
}
