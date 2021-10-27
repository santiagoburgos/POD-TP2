package ar.edu.itba.pod.api.reducers;

import ar.edu.itba.pod.api.OutTreeNeighbourhood;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class MaxPopReducerFactory implements ReducerFactory<String, OutTreeNeighbourhood, OutTreeNeighbourhood> {
@Override
public Reducer<OutTreeNeighbourhood, OutTreeNeighbourhood> newReducer(String key ) {
        return new MaxReducer();
        }
private class MaxReducer extends Reducer<OutTreeNeighbourhood, OutTreeNeighbourhood> {
    private OutTreeNeighbourhood max;
    @Override
    public void beginReduce () {
        max=null;
    }
    @Override
    public void reduce( OutTreeNeighbourhood value ) {
        if(max == null){
            max = value;
        }
        else if(max.getPopulation() < value.getPopulation())
            max = value;
    }
    @Override
    public OutTreeNeighbourhood finalizeReduce() {
        return max;
    }
}
}
