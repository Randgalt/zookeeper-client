/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
