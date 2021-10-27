package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.OutTreeNeighbourhood;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class NeighbourhoodKeyMapper implements Mapper<OutTreeNeighbourhood, Double, String, OutTreeNeighbourhood> {

    @Override
    public void map(OutTreeNeighbourhood key, Double value, Context<String, OutTreeNeighbourhood> context) {
        context.emit(key.getNeighbourhoodName(), new OutTreeNeighbourhood(key.getNeighbourhoodName(),key.getTreeName(),value));
    }

}