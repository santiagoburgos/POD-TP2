package ar.edu.itba.pod.api.collators;

import ar.edu.itba.pod.api.model.PairedValues;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query5Collator implements Collator<Map.Entry<String, Long>, List<PairedValues>> {

    @Override
    public List<PairedValues> collate(Iterable<Map.Entry<String, Long>> values) {
        // Iterable to list filtering those that have value <= 0 and sorting on descending value and then ascending key (street name)
        List<Map.Entry<String, Long>> asList = StreamSupport.stream(values.spliterator(), false)
                .filter(m -> m.getValue() > 0)
                .sorted(Comparator.comparing(Map.Entry<String, Long>::getValue).reversed().thenComparing(Map.Entry::getKey))
                .collect(Collectors.toList());

        // Pairing elements of the list based on value
        List<PairedValues> auxList = new ArrayList<>();
        for (int i = 0; i < asList.size() - 1; i++) {
            for (int j = i + 1; j < asList.size(); j++) {
                Map.Entry<String, Long> firstEntry = asList.get(i);
                Map.Entry<String, Long> secondEntry = asList.get(j);

                if (firstEntry.getValue().equals(secondEntry.getValue())) {
                    auxList.add(new PairedValues(firstEntry.getValue(), firstEntry.getKey(), secondEntry.getKey()));
                }
            }
        }
        // It should technically be sorted already since we sorted when creating asList, and then we iterated over asList in order, but just in case
        Collections.sort(auxList);
        return auxList;
    }
}
