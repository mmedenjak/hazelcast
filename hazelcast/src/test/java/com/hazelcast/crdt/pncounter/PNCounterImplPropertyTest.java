package com.hazelcast.crdt.pncounter;

import com.hazelcast.crdt.BaseCRDTPropertyTest;
import com.hazelcast.crdt.Operation;
import com.hazelcast.crdt.OperationTypes;
import com.hazelcast.crdt.OperationsGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MutableLong;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterImplPropertyTest extends BaseCRDTPropertyTest<PNCounterImpl, MutableLong, Long> {

    private final ILogger logger = Logger.getLogger(PNCounterImplPropertyTest.class);

    @Property
    public void strongEventualConsistency(@InRange(minInt = 2, maxInt = 5)
                                                  int counterCount,
                                          @From(OperationsGenerator.class)
                                          @Size(min = 50, max = 100)
                                          @OperationTypes({AddAndGet.class, GetAndAdd.class, GetAndSubtract.class, SubtractAndGet.class})
                                                  Operation[] operations) {
        super.strongEventualConsistency(counterCount, operations);
    }

    @Override
    protected MutableLong getStateHolder() {
        return new MutableLong();
    }

    @Override
    protected Long getStateValue(PNCounterImpl t) {
        return t.get();
    }

    @Override
    protected Long getStateValue(MutableLong t) {
        return t.value;
    }

    public static class AddAndGet extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public AddAndGet(SourceOfRandomness rnd) {
            super(rnd);
            delta = rnd.nextInt(-100, 100);
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.addAndGet(delta);
            state.value += delta;
        }

        @Override
        public String toString() {
            return "AddAndGet(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class GetAndAdd extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public GetAndAdd(SourceOfRandomness rnd) {
            super(rnd);
            delta = rnd.nextInt(-100, 100);
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.getAndAdd(delta);
            state.value += delta;
        }

        @Override
        public String toString() {
            return "GetAndAdd(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class GetAndSubtract extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public GetAndSubtract(SourceOfRandomness rnd) {
            super(rnd);
            delta = rnd.nextInt(-100, 100);
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.getAndSubtract(delta);
            state.value -= delta;
        }

        @Override
        public String toString() {
            return "GetAndSubtract(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class SubtractAndGet extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public SubtractAndGet(SourceOfRandomness rnd) {
            super(rnd);
            this.delta = rnd.nextInt(-100, 100);
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.subtractAndGet(delta);
            state.value -= delta;
        }

        @Override
        public String toString() {
            return "SubtractAndGet(" + crdtIndex + "," + delta + ")";
        }
    }

    @Override
    protected PNCounterImpl getCRDT(int i) {
        return new PNCounterImpl(i);
    }
}