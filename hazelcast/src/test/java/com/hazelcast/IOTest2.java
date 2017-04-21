/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.Future;

import static java.lang.System.out;

//@RunWith(HazelcastSerialClassRunner.class)
//@Category(com.hazelcast.test.annotation.LateJoinTest.class)
public class IOTest2 {

    public static final int PAGE_SIZE = 1024 * 4;
    //    public static final long FILE_SIZE = PAGE_SIZE * 2000L * 1000L;
    public static final long FILE_SIZE = PAGE_SIZE * 100L * 1000L;
    public static final String FILE_NAME = "test.dat";
    public static final byte[] BLANK_PAGE = new byte[PAGE_SIZE];

//    @Test
    public void testThroughPut() throws Exception {
        for (int threadCount = 1; threadCount < 5; threadCount++) {

            Future[] threads = new Future[threadCount];

            for (int repeatCount = 0; repeatCount < 5; repeatCount++) {
                for (int threadId = 0; threadId < threadCount; threadId++) {
                    final int finalThreadId = threadId;
                    final int finalThreadCount = threadCount;
                    threads[threadId] = HazelcastTestSupport.spawn(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                final String fileName = FILE_NAME + finalThreadId;
                                preallocateTestFile(fileName);
                                final Random rnd = new Random();
                                final byte[] bytes = new byte[PAGE_SIZE];

                                System.gc();
                                long start = System.currentTimeMillis();

                                OutputStream out = new PrintStream(new FileOutputStream(fileName));
                                for (long j = 0; j < FILE_SIZE; j += PAGE_SIZE) {
                                    rnd.nextBytes(bytes);
                                    out.write(bytes);
                                }

                                out.close();
                                long writeDurationMs = System.currentTimeMillis() - start;
                                long bytesWrittenPerSec = (FILE_SIZE * 1000L) / writeDurationMs;
                                System.out.format("%s threads, write=%,d bytes/sec\n", finalThreadCount, bytesWrittenPerSec);

                                deleteFile(fileName);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    });
                }
                for (Future thread : threads) {
                    thread.get();
                }

                System.out.println("--------------------- repeating ---------------------");
            }
            System.out.println("-------------------- increasing threads --------------------");
        }
    }

    private static void preallocateTestFile(final String fileName) throws Exception {
        final RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        for (long i = 0; i < FILE_SIZE; i += PAGE_SIZE) {
            file.write(BLANK_PAGE, 0, PAGE_SIZE);
        }
        file.close();
    }

    private static void deleteFile(final String testFileName) throws Exception {
        final File file = new File(testFileName);
        if (!file.delete()) {
            out.println("Failed to delete test file=" + testFileName);
            out.println("Windows does not allow mapped files to be deleted.");
        }
    }
}