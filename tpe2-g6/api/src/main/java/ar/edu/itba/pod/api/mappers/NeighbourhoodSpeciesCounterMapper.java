package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class NeighbourhoodSpeciesCounterMapper implements Mapper<String, Tree, String, String> {

    @Override
    public void map(String key, Tree tree, Context<String, String> context) {
        context.emit(tree.getNeighbourhood().getName(), tree.getName());
    }
}
