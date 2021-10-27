package ar.edu.itba.pod.api.collators;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KAscCollator <K extends Comparable, V> implements Collator<IMap.Entry<K, V>, List<Map.Entry<K, V>>> {
    @Override
    public List<Map.Entry<K, V>> collate(Iterable<IMap.Entry<K, V>> iterable) {

        List<Map.Entry<K, V>> sorted = StreamSupport.stream(iterable.spliterator(),false).sorted((o1, o2) -> {

            if( o1.getKey() instanceof String )
                return  ((String)o1.getKey()).toLowerCase().compareTo(((String)o2.getKey()).toLowerCase());
            return (o1.getKey()).compareTo(o2.getKey());
        }).collect(Collectors.toList());

        return sorted;
    }
}