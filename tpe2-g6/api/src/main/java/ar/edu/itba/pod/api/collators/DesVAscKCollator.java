package ar.edu.itba.pod.api.collators;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DesVAscKCollator<K extends Comparable, V extends Comparable> implements Collator<IMap.Entry<K, V>, List<Map.Entry<K, V>>> {
    @Override
    public List<Map.Entry<K, V>> collate(Iterable<IMap.Entry<K, V>> iterable) {

        List<Map.Entry<K, V>> sorted = StreamSupport.stream(iterable.spliterator(),false).sorted((o1, o2) -> {

            int valComp = o2.getValue().compareTo(o1.getValue());

            if( o1.getValue() instanceof String )
                valComp =  ((String)o1.getValue()).toLowerCase().compareTo(((String)o2.getValue()).toLowerCase());
            if(valComp != 0)
                return valComp;

            if( o1.getKey() instanceof String )
                return  ((String)o1.getKey()).toLowerCase().compareTo(((String)o2.getKey()).toLowerCase());
            return (o1.getKey()).compareTo(o2.getKey());
        }).collect(Collectors.toList());

        return sorted;

    }
}