package ar.edu.itba.pod.api.collators;

import ar.edu.itba.pod.api.OutTreeNeighbourhood;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MaxTreePerPersonKDescCollator implements Collator<IMap.Entry<OutTreeNeighbourhood, Double>, List<Map.Entry<OutTreeNeighbourhood, Double>>> {
    @Override
    public List<Map.Entry<OutTreeNeighbourhood, Double>> collate(Iterable<IMap.Entry<OutTreeNeighbourhood, Double>> iterable) {

        Map<String, OutTreeNeighbourhood> treesPerPerson = new HashMap<>();
        for(Map.Entry<OutTreeNeighbourhood, Double> t: iterable){
            if(!treesPerPerson.containsKey(t.getKey().getNeighbourhoodName())){
                treesPerPerson.put(t.getKey().getNeighbourhoodName(), new OutTreeNeighbourhood(t.getKey().getNeighbourhoodName(), t.getKey().getTreeName(), t.getValue()));
            }
            else{
                String neighbourhood = t.getKey().getNeighbourhoodName();
                if(treesPerPerson.get(neighbourhood).getPopulation() < t.getValue()  ){
                    treesPerPerson.put(t.getKey().getNeighbourhoodName(), new OutTreeNeighbourhood(t.getKey().getNeighbourhoodName(), t.getKey().getTreeName(), t.getValue()));
                }
            }
        }

        List<Map.Entry<OutTreeNeighbourhood, Double>> sorted = StreamSupport.stream(iterable.spliterator(),false)
                .filter(map -> treesPerPerson.containsValue(map.getKey()) )
                .sorted(Comparator.comparing(o -> (o.getKey().getNeighbourhoodName())))
                .collect(Collectors.toList());

        return sorted;
    }
}
