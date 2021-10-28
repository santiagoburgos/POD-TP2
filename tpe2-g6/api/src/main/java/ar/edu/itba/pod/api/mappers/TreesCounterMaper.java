package ar.edu.itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Collection;
import java.util.HashSet;

public class TreesCounterMaper<K> implements Mapper<K, String, String, Long> {

    private final Collection<String> validNeighbourhoods = new HashSet<>();
    public TreesCounterMaper(Collection<String> neighbourhoods) {
        validNeighbourhoods.addAll(neighbourhoods);
    }

    private static final Long ONE = 1L;

    @Override
    public void map(K key, String value, Context<String, Long> context){
        if(validNeighbourhoods.contains(value))
        context.emit(value,ONE);
    }
}