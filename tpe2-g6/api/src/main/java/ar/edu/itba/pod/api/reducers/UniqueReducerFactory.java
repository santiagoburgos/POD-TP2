package ar.edu.itba.pod.api.reducers;

import ar.edu.itba.pod.api.model.Neighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class UniqueReducerFactory implements ReducerFactory<String, String, Integer> {

    @Override
    public Reducer<String, Integer> newReducer(String neighbourhood) {
        return new UniqueReducer();
    }

    private static class UniqueReducer extends Reducer<String, Integer> {

        private Set<String> uniqueTrees;

        @Override
        public void beginReduce() {
            uniqueTrees = new HashSet<>();
        }

        @Override
        public void reduce(String s) {
            uniqueTrees.add(s);
        }

        @Override
        public Integer finalizeReduce() {
            return uniqueTrees.size();
        }
    }
}
