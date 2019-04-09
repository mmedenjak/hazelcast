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
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SPLIT_BRAIN_HANDLER_EXECUTOR_NAME;

public class ChangeConfigAndUpgradeOp extends AbstractClusterOperation {

    private Address targetAddress;
    private Config newConfig;

    public ChangeConfigAndUpgradeOp() {
    }

    public ChangeConfigAndUpgradeOp(Config newConfig, Address targetAddress) {
        this.newConfig = newConfig;
        this.targetAddress = targetAddress;
    }

    @Override
    public void run() throws Exception {
        final Address caller = getCallerAddress();
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        final ClusterServiceImpl clusterService = node.getClusterService();
        final Address masterAddress = clusterService.getMasterAddress();
        final ILogger logger = node.loggingService.getLogger(this.getClass().getName());

        boolean local = caller == null;
        if (!local && !caller.equals(masterAddress)) {
            logger.warning("Ignoring upgrade config and merge instruction sent from non-master endpoint: " + caller);
            return;
        }

        logger.warning(node.getThisAddress() + " is upgrading config and merging to " + targetAddress
                + ", because: instructed by master " + masterAddress);

        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_HANDLER_EXECUTOR_NAME, new Runnable() {
            @Override
            public void run() {
                clusterService.upgradeAndMerge(newConfig, targetAddress);
            }
        });
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
        return ClusterDataSerializerHook.CHANGE_CONFIG_AND_UPGRADE_OP;
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
        out.writeObject(targetAddress);
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
        targetAddress = in.readObject();
        newConfig.setNativeMemoryConfig(in.<NativeMemoryConfig>readObject());
        JoinConfig joinConfig = newConfig.getNetworkConfig().getJoin();
        joinConfig.setDiscoveryConfig(in.<DiscoveryConfig>readObject());
        joinConfig.getTcpIpConfig().setEnabled(in.readBoolean());
        joinConfig.getMulticastConfig().setEnabled(in.readBoolean());
        newConfig.setProperties(in.<Properties>readObject());
    }
}
