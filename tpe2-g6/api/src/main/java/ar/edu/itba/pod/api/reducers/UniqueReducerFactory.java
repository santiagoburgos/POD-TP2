package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class UniqueReducerFactory implements ReducerFactory<String, String, Long> {

    private final Long floorValue;

    public UniqueReducerFactory() {
        this.floorValue = 1L;
    }

    public UniqueReducerFactory(Long floorValue) {
        this.floorValue = floorValue;
    }

    @Override
    public Reducer<String, Long> newReducer(String neighbourhood) {
        return new UniqueReducer(floorValue);
    }

    private static class UniqueReducer extends Reducer<String, Long> {
        private final Long floorValue;
        private Set<String> uniqueTrees;

        public UniqueReducer(Long floorValue) {
            this.floorValue = floorValue;
        }

        @Override
        public void beginReduce() {
            uniqueTrees = new HashSet<>();
        }

        @Override
        public void reduce(String s) {
            uniqueTrees.add(s);
        }

        @Override
        public Long finalizeReduce() {
            if (floorValue > 1L)
                return (Math.floorDiv(uniqueTrees.size(), this.floorValue)) * this.floorValue;
            return (long) uniqueTrees.size();
        }
    }
}
