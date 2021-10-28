package ar.edu.itba.pod.api.reducers;

import ar.edu.itba.pod.api.model.Q2NeighbourhoodTree;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2MaxPopReducerFactory implements ReducerFactory<String, Q2NeighbourhoodTree, Q2NeighbourhoodTree> {
@Override
public Reducer<Q2NeighbourhoodTree, Q2NeighbourhoodTree> newReducer(String key ) {
        return new MaxReducer();
        }
private class MaxReducer extends Reducer<Q2NeighbourhoodTree, Q2NeighbourhoodTree> {
    private Q2NeighbourhoodTree max;
    @Override
    public void beginReduce () {
        max=null;
    }
    @Override
    public void reduce( Q2NeighbourhoodTree value ) {
        if(max == null){
            max = value;
        }
        else if(max.getPopulation() < value.getPopulation())
            max = value;
    }
    @Override
    public Q2NeighbourhoodTree finalizeReduce() {
        return max;
    }
}
}
