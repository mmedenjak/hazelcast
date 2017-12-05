package com.hazelcast.crdt;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.lang.reflect.Constructor;

public class OperationsGenerator extends Generator<Operation[]> {
    private Class<? extends Operation>[] types;
    private int min;
    private int max;

    public OperationsGenerator() {
        super(Operation[].class);
    }

    @Override
    public Operation[] generate(SourceOfRandomness r, GenerationStatus status) {
        final int size = r.nextInt(min, max);
        final Operation[] operations = new Operation[size];

        for (int i = 0; i < size; i++) {
            if (r.nextDouble() < 0.2) {
                operations[i] = new MergingOperation(r);
            } else {
                try {
                    final Class operationClass = r.choose(types);
                    final Constructor<Operation> c = operationClass.getConstructor(SourceOfRandomness.class);
                    operations[i] = c.newInstance(r);
                } catch (Exception e) {
                    throw new RuntimeException("Error instantiating the operation ", e);
                }
            }
        }
        return operations;
    }

    public void configure(OperationTypes types) {
        this.types = types.value();
    }

    public void configure(Size size) {
        min = size.min();
        max = size.max();
    }
}