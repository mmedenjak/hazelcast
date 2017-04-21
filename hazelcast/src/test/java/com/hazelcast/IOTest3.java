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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.System.out;

//@RunWith(HazelcastSerialClassRunner.class)
//@Category(com.hazelcast.test.annotation.LateJoinTest.class)
public class IOTest3 {

    public static final int PAGE_SIZE = 1024 * 4;
    //    public static final long FILE_SIZE = PAGE_SIZE * 2000L * 1000L;
    public static final long FILE_SIZE = PAGE_SIZE * 100L * 1000L;
    public static final String FILE_NAME = "test.dat";
    public static final byte[] BLANK_PAGE = new byte[PAGE_SIZE];
    public static final int TEST_DURATION_MINUTES = 60 * 2;

//    @Test(timeout = 1000 * 60 * TEST_DURATION_MINUTES) // 2 hours
    public void testThroughPut() throws Exception {
        final long testStart = System.currentTimeMillis();
        preallocateTestFile(FILE_NAME);
        long passedMinutes = 0;
        do {
            passedMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - testStart);
            try {
                System.out.format(passedMinutes + " write with buffer=%,d bytes/sec\n", (FILE_SIZE * 1000L) / write(true));
                System.out.format(passedMinutes + " write without buffer=%,d bytes/sec\n", (FILE_SIZE * 1000L) / write(false));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            deleteFile(FILE_NAME);
        } while (passedMinutes < TEST_DURATION_MINUTES);
    }

    private long write(boolean buffer) throws IOException {
        final Random rnd = new Random();
        final byte[] bytes = new byte[PAGE_SIZE];

        System.gc();
        long start = System.currentTimeMillis();

        OutputStream out = new FileOutputStream(FILE_NAME);
        if (buffer) {
            out = new BufferedOutputStream(out);
        }
        for (long j = 0; j < FILE_SIZE; j += PAGE_SIZE) {
            rnd.nextBytes(bytes);
            out.write(bytes);
        }

        out.close();
        return System.currentTimeMillis() - start;
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