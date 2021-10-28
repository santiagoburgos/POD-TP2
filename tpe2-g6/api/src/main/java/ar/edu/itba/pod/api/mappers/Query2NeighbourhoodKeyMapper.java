package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.Q2NeighbourhoodTree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2NeighbourhoodKeyMapper implements Mapper<Q2NeighbourhoodTree, Double, String, Q2NeighbourhoodTree> {

    @Override
    public void map(Q2NeighbourhoodTree key, Double value, Context<String, Q2NeighbourhoodTree> context) {
        context.emit(key.getNeighbourhoodName(), new Q2NeighbourhoodTree(key.getNeighbourhoodName(),key.getTreeName(),value));
    }

}