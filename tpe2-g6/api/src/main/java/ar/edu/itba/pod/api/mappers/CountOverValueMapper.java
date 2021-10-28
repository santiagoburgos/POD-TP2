package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CountOverValueMapper<K> implements Mapper<K, PairCompoundKeyValue, PairCompoundKeyValue, Double> {

    @Override
    public void map(K key, PairCompoundKeyValue value, Context<PairCompoundKeyValue, Double> context) {
        context.emit(value, 1d/value.getValue());
    }

}
