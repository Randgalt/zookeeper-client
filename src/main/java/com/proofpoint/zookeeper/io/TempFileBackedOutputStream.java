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
package com.proofpoint.zookeeper.io;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * An output stream that writes, first, to a temp file. When completed, the temp
 * file is renamed to the desired destination file. This emulates an atomic write.
 * <p/>
 * Canonical usage:<br/>
<code><pre>
TempFileBackedOutputStream      stream = ...
try
{
    stream.write(...);
    ...
    stream.commit();
}
finally
{
    stream.release();
}
</pre></code>
 */
public interface TempFileBackedOutputStream
{
    /**
     * Returns the output stream. This can safely be called multiple times
     *
     * @return stream.
     */
    DataOutputStream getStream();

    void commit()
            throws IOException;

    void release()
            throws IOException;
}
