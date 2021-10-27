package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.OutTreeNeighbourhood;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CounterOverPopulationMapper<K> implements Mapper<K, OutTreeNeighbourhood, OutTreeNeighbourhood, Double> {

    @Override
    public void map(K key, OutTreeNeighbourhood value, Context<OutTreeNeighbourhood, Double> context) {
        context.emit(value, 1d/value.getPopulation());
    }

}
