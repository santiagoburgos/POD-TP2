package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Key1PairMapper implements Mapper<PairCompoundKeyValue, Double, String, PairCompoundKeyValue> {

    @Override
    public void map(PairCompoundKeyValue key, Double value, Context<String, PairCompoundKeyValue> context) {
        context.emit(key.getK1(), new PairCompoundKeyValue(key.getK1(),key.getK2(),value));
    }

}