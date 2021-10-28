package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

public class Query5Predicate implements KeyPredicate<String> {

    private final String valid;

    public Query5Predicate(String valid) {
        this.valid = valid != null ? valid.toLowerCase() : "";
    }

    @Override
    public boolean evaluate(String s) {
        return valid.equals(s.toLowerCase());
    }
}
