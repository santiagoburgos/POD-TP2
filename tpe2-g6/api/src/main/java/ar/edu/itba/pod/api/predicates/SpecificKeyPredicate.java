package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

public class SpecificKeyPredicate implements KeyPredicate<String> {

    private final String valid;

    public SpecificKeyPredicate(String valid) {
        this.valid = valid != null ? valid.toLowerCase() : "";
    }

    @Override
    public boolean evaluate(String s) {
        return valid.equals(s.toLowerCase());
    }
}
