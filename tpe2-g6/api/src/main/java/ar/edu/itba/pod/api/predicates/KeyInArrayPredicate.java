package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

public class KeyInArrayPredicate implements KeyPredicate<String> {
    private final Collection<String> validKeys = new HashSet<>();

    public KeyInArrayPredicate(Collection<String> validKeys) {
        this.validKeys.addAll(validKeys.stream().map(String::toLowerCase).collect(Collectors.toList()));
    }

    @Override
    public boolean evaluate(String s) {
        return validKeys.contains(s.toLowerCase());
    }
}
