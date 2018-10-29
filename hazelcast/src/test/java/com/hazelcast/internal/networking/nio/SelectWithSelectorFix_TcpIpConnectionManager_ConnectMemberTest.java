/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.nio.tcp.TcpIpConnectionManager_AbstractConnectMemberTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SelectWithSelectorFix_TcpIpConnectionManager_ConnectMemberTest
        extends TcpIpConnectionManager_AbstractConnectMemberTest {

    @Test
    public void testLala() throws Exception {
        int i = 0;
        while (i < 1000 * 1000) {
            if (i++ % 10000 == 0) {
                System.out.println("Running " + i);
            }
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            ServerSocket serverSocket = serverSocketChannel.socket();
            serverSocket.setReuseAddress(true);
            serverSocket.setSoTimeout(1000);
            serverSocket.bind(new InetSocketAddress("0.0.0.0", 5701));

            serverSocketChannel.close();

//
//            LoggingServiceImpl serice = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo());
//            MetricsRegistryImpl reg = new MetricsRegistryImpl(serice.getLogger(MetricsRegistryImpl.class), INFO);
//            TcpIpConnectionManager cm = newConnectionManager(5701, reg);
//            cm.start();
//            cm.shutdown();
        }
    }

    //@Before
    public void setup() throws Exception {
        networkingFactory = new SelectWithSelectorFix_NioNetworkingFactory();
        super.setup();
    }
}
