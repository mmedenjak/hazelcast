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

package com.hazelcast.config;

public final class OOMEProtectionConfig {
    private boolean enabled = false;
    private int checkerPeriod = 0;
    private double minFreePercentage = 10;
    private double evictPercentage = 20;

    public OOMEProtectionConfig() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getCheckerPeriod() {
        return checkerPeriod;
    }

    public OOMEProtectionConfig setCheckerPeriod(int checkerPeriod) {
        this.checkerPeriod = checkerPeriod;
        return this;
    }

    public double getMinFreePercentage() {
        return minFreePercentage;
    }

    public OOMEProtectionConfig setMinFreePercentage(double minFreePercentage) {
        this.minFreePercentage = minFreePercentage;
        return this;
    }

    public double getEvictPercentage() {
        return evictPercentage;
    }

    public OOMEProtectionConfig setEvictPercentage(double evictPercentage) {
        this.evictPercentage = evictPercentage;
        return this;
    }
}
