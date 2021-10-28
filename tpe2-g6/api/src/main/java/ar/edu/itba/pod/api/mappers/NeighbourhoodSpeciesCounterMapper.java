package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Collection;
import java.util.HashSet;

public class NeighbourhoodSpeciesCounterMapper implements Mapper<String, Tree, String, String> {

    private final Collection<String> validNeighbourhoods = new HashSet<>();
    public NeighbourhoodSpeciesCounterMapper(Collection<String> neighbourhoods) {
        validNeighbourhoods.addAll(neighbourhoods);
    }

    @Override
    public void map(String key, Tree tree, Context<String, String> context) {
        if (validNeighbourhoods.contains(tree.getNeighbourhood().getName())) {
            context.emit(tree.getNeighbourhood().getName(), tree.getName());
        }
    }
}
