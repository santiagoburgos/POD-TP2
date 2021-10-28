package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5ReducerFactory implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String s) {
        return new Query5Reducer();
    }

    private class Query5Reducer extends Reducer<Long, Long> {
        private volatile long sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Long aLong) {
            sum += aLong;
        }

        @Override
        public Long finalizeReduce() {
            return (Math.floorDiv(sum, 100L)) * 100;
        }
    }
}
