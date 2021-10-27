package ar.edu.itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CounterMapper<K, V> implements Mapper<K, V, V, Long> {
    private static final Long ONE = 1L;

        @Override
        public void map(K key, V value, Context<V, Long> context) {
                context.emit(value, ONE);
            }

    }
