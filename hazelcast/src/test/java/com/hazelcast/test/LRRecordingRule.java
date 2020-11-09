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

package com.hazelcast.test;

import io.undo.lr.UndoLR;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static com.hazelcast.internal.nio.IOUtil.toFileName;

public class LRRecordingRule implements TestRule {
    @Override
    public Statement apply(final Statement base, final Description description) {
        if (!Boolean.getBoolean("undo.enabled")) {
            return base;
        }

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                UndoLR.start();
                try {
                    base.evaluate();
                } finally {
                    String fname = toFileName(description.getTestClass().getSimpleName())
                            + '_' + toFileName(description.getMethodName())
                            + '_' + System.currentTimeMillis() + ".undo";
                    UndoLR.save(fname);
                }
            }
        };
    }
}
