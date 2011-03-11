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

import com.google.inject.Inject;
import com.proofpoint.log.Logger;
import org.apache.commons.io.FileUtils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Abstraction for managing temporary directories used for interacting with Lucene, etc.
 * <p/>
 * Usage:
 * <p/>
 * <pre>
 * TempLocalDirectory tmp = new TempLocalDirectory()
 * try {
 *    tmp.newFile()
 *    tmp.newDirectory()
 * }
 * finally {
 *    tmp.cleanup();
 * }
 * </pre>
 */
public class TempLocalDirectory
{
    private static final Logger log = Logger.get(TempLocalDirectory.class);

    private static final String PREFIX = "temp_";
    private static final String SUFFIX = ".tmp";

    private final File path;

    @Inject
    public TempLocalDirectory()
            throws IOException
    {
        path = createTempDir(null);
        log.debug("Created local temp dir: " + path.getAbsolutePath());
    }

    private File createTempDir(File parent)
            throws IOException
    {
        File dir;
        if (parent == null) {
            dir = File.createTempFile(PREFIX, SUFFIX);
        }
        else {
            dir = File.createTempFile(PREFIX, SUFFIX, parent);
        }

        if (!dir.delete()) {
            throw new IOException("Could not delete temp file: " + path.getAbsolutePath());
        }
        if (!dir.mkdir()) {
            throw new IOException("Could not create temp dir: " + path.getAbsolutePath());
        }

        return dir;
    }

    public void cleanupPrevious()
    {
        try {
            FileUtils.cleanDirectory(path);
            log.debug("Cleaned up local temp dir: " + path.getAbsolutePath());
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning up temp local directory: %s", path.getAbsolutePath());
            throw new Error(e);
        }
    }

    public void cleanup()
    {
        try {
            FileUtils.deleteDirectory(path);
            log.debug("Cleaned up local temp dir: " + path.getAbsolutePath());
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning up temp local directory: %s", path.getAbsolutePath());
            throw new Error(e);
        }
    }

    public File newFile()
            throws IOException
    {
        return File.createTempFile(PREFIX, SUFFIX, path);
    }

    private void deleteFile(File file)
            throws IOException
    {
        if (!file.delete()) {
            throw new IOException("Could not delete: " + path);
        }
    }

    public File newDirectory()
            throws IOException
    {
        return createTempDir(path);
    }

    public void deleteDirectory(File dir)
    {
        try {
            FileUtils.deleteDirectory(dir);
        }
        catch (IOException e) {
            // throwing an excepiton is useless - just record it
            log.error(e, "Cleaning up temp dir: %s", dir);
        }
    }

    public TempFileBackedOutputStream newTempFileBackedOutputStream(String destinationPath)
            throws IOException
    {
        final File tempFile = newFile();
        final File destinationFile = new File(destinationPath);
        final DataOutputStream tempFileOutputStream = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tempFile)));

        return new TempFileBackedOutputStream()
        {
            @Override
            public DataOutputStream getStream()
            {
                return tempFileOutputStream;
            }

            @Override
            public void commit()
                    throws IOException
            {
                close(true);
            }

            @Override
            public void release()
                    throws IOException
            {
                close(false);
            }

            private void close(boolean upload)
                    throws IOException
            {
                if (isOpen) {
                    isOpen = false;

                    tempFileOutputStream.close();
                    if (upload) {
                        if (!tempFile.renameTo(destinationFile)) {
                            throw new IOException(
                                    "Could not rename " + tempFile.getPath() + " to " + destinationFile.getPath());
                        }
                    }
                    else {
                        deleteFile(tempFile);
                    }
                }
            }

            private boolean isOpen = true;
        };
    }
}
