/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import jdk.jfr.Configuration;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;

public final class JfrSupport {

    public static boolean isJfrEnabled() {
        return FlightRecorder.isAvailable() && FlightRecorder.isInitialized();
    }

    public static boolean isJfrAvailable() {
        return FlightRecorder.isAvailable();
    }

    public static void startRecording() throws IOException, ParseException {
        startRecording("default");
    }

    public static Recording startRecording(String configName) throws IOException, ParseException {
        ILogger logger = Logger.getLogger(JfrSupport.class);
        Configuration jfrConfig = Configuration.getConfiguration(configName);
        Recording recording = new Recording(jfrConfig);
        recording.setMaxAge(Duration.ofHours(1));
        recording.setToDisk(false);
        recording.setDumpOnExit(true);
        recording.setDestination(Paths.get("projectx-HFR.jfr"));
        recording.start();

        logger.info(String.format("Started JFR recording with the following configuration\n"
                        + "\tProvider: %s\n"
                        + "\tName: %s\n"
                        + "\tLabel: %s\n"
                        + "\tDescription: %s\n"
                        + "and recording parameters:\n"
                        + "\tName: %s\n"
                        + "\tUnique ID: %s\n"
                        + "\tMaximum age: %s\n"
                        + "\tPersisting to disk continuously: %s\n"
                        + "\tDestination: %s\n"
                        + "\tDump on exit: %s\n"
                        + " ", jfrConfig.getProvider(), jfrConfig.getName(), jfrConfig.getLabel(), jfrConfig.getDescription(),
                recording.getName(), recording.getId(), recording.getMaxAge(), recording.isToDisk(),
                recording.getDestination().toAbsolutePath(), recording.getDumpOnExit()));

        return recording;
    }

    public static void dumpRecording(Recording recording, String path) throws IOException, ParseException {
        //        recording.
    }

    private JfrSupport() {
    }
}
