/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib;

import org.apache.spark.annotation.AlphaComponent;

import scala.reflect.internal.Trees.Try;
import scala.util.control.Exception.Finally;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * A dummy class as a workaround to show the package doc of
 * <code>spark.mllib</code> in generated Java API docs.
 *
 * @see <a href="http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4492654"
 *      target="_blank"> JDK-4492654</a>
 */
@AlphaComponent
public class JavaPackage {
    private JavaPackage() {
    }

    private static boolean IsEnvironSet = false;

    private static void SetEnviron() {
        if (IsEnvironSet == false) {
            IsEnvironSet = true;

            System.setProperty("tornado.load.api.implementation",
                    "uk.ac.manchester.tornado.runtime.tasks.TornadoTaskSchedule");
            System.setProperty("tornado.load.runtime.implementation",
                    "uk.ac.manchester.tornado.runtime.TornadoCoreRuntime");
            System.setProperty("tornado.load.tornado.implementation",
                    "uk.ac.manchester.tornado.runtime.common.Tornado");
            System.setProperty("tornado.load.device.implementation.opencl",
                    "uk.ac.manchester.tornado.drivers.opencl.runtime.OCLDeviceFactory");
            System.setProperty("tornado.load.device.implementation.ptx",
                    "uk.ac.manchester.tornado.drivers.ptx.runtime.PTXDeviceFactory");
            System.setProperty("tornado.load.annotation.implementation",
                    "uk.ac.manchester.tornado.annotation.ASMClassVisitor");
            System.setProperty("tornado.load.annotation.parallel", "uk.ac.manchester.tornado.api.annotations.Parallel");
        }
    }

    private static void _sumPositive(@Reduce double[] ret, final double maxMargin, double[] margins,
            final int maxMarginIndex) {
        ret[0] = 0;
        for (@Parallel
        int i = 0; i < margins.length; i++) {
            margins[i] -= maxMargin;
            /*
             * double num = 0; if (i == maxMarginIndex) { num = -maxMargin; } else { num =
             * margins[i]; }
             */
            final double num = (i == maxMarginIndex) ? -maxMargin : margins[i];
            ret[0] += Math.exp(num);
        }
    }

    private static void _sumNonPositive(@Reduce double[] ret, double[] margins) {
        ret[0] = 0;
        for (@Parallel
        int i = 0; i < margins.length; i++) {
            ret[0] += Math.exp(margins[i]);
        }
    }

    private static FileLock lock() {
        FileLock lock = null;
        try {
            String username = System.getProperty("user.name");
            RandomAccessFile raf = new RandomAccessFile("/home/" + username + "/spark/lockfile", "rw");
            FileChannel channel = raf.getChannel();

            while (true) {
                int locktry = 0;
                try {
                    locktry++;
                    while (lock == null) {
                        // Thread.sleep(5);
                        lock = channel.tryLock();
                    }
                    break;
                } catch (OverlappingFileLockException e) {
                    if (locktry % 128 == 0) {
                        System.out.println("[JJ] try lock " + e.toString());
                    }
                } catch (Exception e) {
                    System.out.println("[JJ] Something Wrong0 " + e.toString());
                }
            }
        } catch (IOException e) {
            System.out.println("[JJ] FileNotFound " + e.toString());
        }

        return lock;
    }

    public static double sum(double maxMargin, int numClasses, double[] margins, int maxMarginIndex) {
        boolean useTornadoVM = true;

        double[] ret = new double[1];
        ret[0] = 0;

        if (useTornadoVM) {
            SetEnviron();

            long startTime = 0;
            long endTime = 0;
            long elapsed = 0;
            TaskSchedule t;

            FileLock exclusivelock = lock();

            System.out.printf("[JJ] margins size: %d\n", Double.BYTES * margins.length);

            if (maxMargin > 0) {
                t = new TaskSchedule("s0").streamIn(margins)
                        .task("t0", JavaPackage::_sumPositive, ret, maxMargin, margins, maxMarginIndex)
                        .streamOut(ret, margins);
            } else {
                t = new TaskSchedule("s0").streamIn(margins).task("t0", JavaPackage::_sumNonPositive, ret, margins)
                        .streamOut(ret);
            }

            startTime = System.nanoTime();
            t.execute();
            endTime = System.nanoTime();

            t.getDevice().reset();

            try {
                exclusivelock.release();
            } catch (Exception e) {
                System.out.println("[JJ] unlock " + e.toString());
            }

            elapsed = endTime - startTime;

            System.out.println("[JJ] TornadoVM Execution Sum: " + (elapsed / 1000000));
        } else {
            if (maxMargin > 0) {
                _sumPositive(ret, maxMargin, margins, maxMarginIndex);
            } else {
                _sumNonPositive(ret, margins);
            }
        }
        return ret[0];
    }
}
