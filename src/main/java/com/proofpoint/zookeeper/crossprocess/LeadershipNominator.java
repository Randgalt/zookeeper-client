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
package com.proofpoint.zookeeper.crossprocess;

public interface LeadershipNominator
{
    /**
     * Set the notifier to be called when leadership is obtained
     *
     * @param n notifier
     */
    void     setNotifier(LeadershipNominatorNotifier n);

    /**
     * Returns the currently set notifier
     *
     * @return notifier
     */
    LeadershipNominatorNotifier getNotifier();

    /**
     * Start the nominator
     */
    void        start();

    /**
     * Shutdown the group. Interrupt the current leader.
     */
    void        stop();

    /**
     * Returns true if this instance currently has leadership
     *
     * @return true/false
     */
    boolean     hasLeadership();
}
