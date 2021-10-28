package ar.edu.itba.pod.api.reducers;

import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class MaxValueReducerFactory implements ReducerFactory<String, PairCompoundKeyValue, PairCompoundKeyValue> {
@Override
public Reducer<PairCompoundKeyValue, PairCompoundKeyValue> newReducer(String key ) {
        return new MaxReducer();
        }
private class MaxReducer extends Reducer<PairCompoundKeyValue, PairCompoundKeyValue> {
    private PairCompoundKeyValue max;
    @Override
    public void beginReduce () {
        max=null;
    }
    @Override
    public void reduce( PairCompoundKeyValue value ) {
        if(max == null){
            max = value;
        }
        else if(max.getValue() < value.getValue())
            max = value;
        else if(max.getValue().equals(value.getValue()) )
            if(max.getK2().toLowerCase().compareTo(value.getK2().toLowerCase()) > 0)
                max = value;
    }
    @Override
    public PairCompoundKeyValue finalizeReduce() {
        return max;
    }
}
}
