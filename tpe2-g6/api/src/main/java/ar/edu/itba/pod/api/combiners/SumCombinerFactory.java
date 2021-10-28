package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class SumCombinerFactory implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String s) {
        return new SumCombiner();
    }

    private class SumCombiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public void combine(Long aLong) {
            sum++;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
