package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Collection;
import java.util.HashSet;

public class CountOverValueMapper<K> implements Mapper<K, PairCompoundKeyValue, PairCompoundKeyValue, Double> {

    private final Collection<String> validNeighbourhoods = new HashSet<>();
    public CountOverValueMapper(Collection<String> neighbourhoods) {
        validNeighbourhoods.addAll(neighbourhoods);
    }

    @Override
    public void map(K key, PairCompoundKeyValue value, Context<PairCompoundKeyValue, Double> context) {
        if(validNeighbourhoods.contains(value.getK1())) {
            context.emit(value, 1d / value.getValue());
        }
    }

}
