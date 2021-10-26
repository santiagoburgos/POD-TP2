package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class SumReducerFactory<K> implements ReducerFactory<K, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(K key ) {
        return new SumReducer();
    }
    private class SumReducer extends Reducer<Long, Long> {
        private long sum;
        @Override
        public void beginReduce () {
            sum = 0;
        }
        @Override
        public void reduce( Long value ) {
            sum += value.longValue();
        }
        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
