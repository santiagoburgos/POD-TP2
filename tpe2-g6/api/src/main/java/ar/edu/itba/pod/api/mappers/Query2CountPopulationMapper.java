package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.Q2NeighbourhoodTree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2CountPopulationMapper<K> implements Mapper<K, Q2NeighbourhoodTree, Q2NeighbourhoodTree, Double> {

    @Override
    public void map(K key, Q2NeighbourhoodTree value, Context<Q2NeighbourhoodTree, Double> context) {
        context.emit(value, 1d/value.getPopulation());
    }

}
