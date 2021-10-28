package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TopNCollator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    private final Integer n;

    public TopNCollator(Integer n) {
        this.n = n;
    }

    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        return StreamSupport.stream(iterable.spliterator(),false)
                .sorted(getUniqueSpeciesComparator())
                .limit(n)
                .collect(Collectors.toList());
    }

    private Comparator<Map.Entry<String, Long>> getUniqueSpeciesComparator() {
        return (e1, e2) -> {
            if(e1.getValue().equals(e2.getValue()))
                return String.CASE_INSENSITIVE_ORDER.compare(e1.getKey(), e2.getKey());
            else
                return Double.compare(e2.getValue(), e1.getValue());
        };
    }
}
