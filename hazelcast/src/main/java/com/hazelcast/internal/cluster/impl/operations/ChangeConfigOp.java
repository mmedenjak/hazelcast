/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public class ChangeConfigOp extends AbstractClusterOperation {

    private Config newConfig;
    private ILogger logger;
    private final FutureUtil.ExceptionHandler splitBrainMergeExceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof MemberLeftException) {
                return;
            }
            logger.warning("Problem while waiting for merge operation result", throwable);
        }
    };

    public ChangeConfigOp() {
    }

    public ChangeConfigOp(Config newConfig) {
        this.newConfig = newConfig;
    }

    @Override
    public void run() throws Exception {
        logger = getNodeEngine().getLogger(this.getClass());
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Member> memberList = getNodeEngine().getClusterService().getMembers();
        Collection<Future> futures = new ArrayList<Future>(memberList.size());

        Address newMasterAddress = null;
        for (Member member : memberList) {
            if (!member.localMember()) {
                Operation op = new ChangeConfigAndUpgradeOp(newConfig, newMasterAddress);
                Future<Object> future =
                        operationService.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, member.getAddress());
                if (newMasterAddress == null) {
                    waitWithDeadline(Collections.singleton(future), 30, TimeUnit.SECONDS, splitBrainMergeExceptionHandler);
                    newMasterAddress = member.getAddress();
                } else {
                    futures.add(future);
                }
            }
        }

        waitWithDeadline(futures, 30, TimeUnit.SECONDS, splitBrainMergeExceptionHandler);

        Operation op = new ChangeConfigAndUpgradeOp(newConfig, newMasterAddress);
        op.setNodeEngine(getNodeEngine())
          .setService(getNodeEngine().getClusterService())
          .setOperationResponseHandler(createEmptyResponseHandler());
        operationService.run(op);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.CHANGE_CONFIG_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        Map<String, MapConfig> mapConfigs = newConfig.getMapConfigs();
        out.writeInt(mapConfigs.size());
        for (Entry<String, MapConfig> entry : mapConfigs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeObject(newConfig.getNativeMemoryConfig());
        JoinConfig join = newConfig.getNetworkConfig().getJoin();
        out.writeObject(join.getDiscoveryConfig());
        out.writeBoolean(join.getTcpIpConfig().isEnabled());
        out.writeBoolean(join.getMulticastConfig().isEnabled());
        out.writeObject(newConfig.getProperties());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newConfig = new Config();
        int size = in.readInt();
        Map<String, MapConfig> configs = newConfig.getMapConfigs();
        for (int i = 0; i < size; i++) {
            configs.put(in.readUTF(), in.<MapConfig>readObject());
        }
        newConfig.setNativeMemoryConfig(in.<NativeMemoryConfig>readObject());
        JoinConfig joinConfig = newConfig.getNetworkConfig().getJoin();
        joinConfig.setDiscoveryConfig(in.<DiscoveryConfig>readObject());
        joinConfig.getTcpIpConfig().setEnabled(in.readBoolean());
        joinConfig.getMulticastConfig().setEnabled(in.readBoolean());
        newConfig.setProperties(in.<Properties>readObject());
    }
}
