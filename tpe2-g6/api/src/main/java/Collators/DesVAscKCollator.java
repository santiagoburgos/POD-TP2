package Collators;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DesVAscKCollator<K extends Comparable, V extends Comparable> implements Collator<IMap.Entry<K, V>, List<Map.Entry<K, V>>> {
    @Override
    public List<Map.Entry<K, V>> collate(Iterable<IMap.Entry<K, V>> iterable) {

        List<Map.Entry<K, V>> sorted = StreamSupport.stream(iterable.spliterator(),false).sorted(new Comparator<IMap.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {

                int valComp = o2.getValue().compareTo(o1.getValue());
                if(valComp != 0)
                    return valComp;

                return (o1.getKey()).compareTo(o2.getKey());
            }
        }).collect(Collectors.toList());

    return sorted;

    }
}
