package Collators;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DesVAscKCollator implements Collator<IMap.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<IMap.Entry<String, Long>> iterable) {

        List<Map.Entry<String, Long>> sorted = StreamSupport.stream(iterable.spliterator(),false).sorted(new Comparator<IMap.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {

                int valComp = o2.getValue().compareTo(o1.getValue());
                if(valComp != 0)
                    return valComp;

                return (o1.getKey()).compareTo(o2.getKey());
            }
        }).collect(Collectors.toList());

    return sorted;

    }
}
