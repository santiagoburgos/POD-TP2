package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.model.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5Mapper implements Mapper<String, Tree, String, Long> {
    private static final Long ONE = 1L;
    private final String name;

    public Query5Mapper(String name) {
        this.name = name;
    }

    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if (tree.getName().equals(this.name)) {
            context.emit(tree.getStreet(), ONE);
        }
    }
}
