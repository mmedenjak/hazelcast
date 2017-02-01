package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class EntryProcessorPerformanceTest extends HazelcastTestSupport {

    private static final int WARMUP_ITERATIONS_COUNT = 1000;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 5000;

    private HazelcastInstance hz;
    private IMap<Integer, PortableEmployee> map;


    @Setup
    public void setup() throws IOException {
        final Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(1, new PortableFactory() {
            public Portable create(int classId) {
                return classId == 2 ? new PortableEmployee() : new PortableDuty();
            }
        });
        hz = createHazelcastInstance(config);
        map = hz.getMap("map");

        final Random rnd = new Random();
        for (int i = 0; i < 1000; i++) {

            final int dutyCnt = rnd.nextInt(10);
            final Portable[] duties = new Portable[dutyCnt];
            for (int j = 0; j < dutyCnt; j++) {
                duties[j] = new PortableDuty("Some random duty", rnd.nextInt(5));
            }
            map.put(i, new PortableEmployee(i % 90, true, i, String.valueOf("Matko " + i), duties));
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        hz.shutdown();
    }

    @Benchmark
    public Object executeOnEntries() throws IOException {
        return map.executeOnEntries(new AbstractEntryProcessor<Integer, PortableEmployee>() {
            @Override
            public Object process(Map.Entry<Integer, PortableEmployee> entry) {
                final PortableEmployee val = entry.getValue();
                if (val.getAge() > 60) {
                    val.setIsEmployee(false);
                }
                return null;
            }
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EntryProcessorPerformanceTest.class.getSimpleName())
                .warmupIterations(WARMUP_ITERATIONS_COUNT)
                .warmupTime(TimeValue.milliseconds(2))
                .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
                .measurementTime(TimeValue.milliseconds(2))
                .verbosity(VerboseMode.NORMAL)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
