package mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CounterMapper implements Mapper<String, String, String, Long> {
    private static final Long ONE = 1L;

        @Override
        public void map(String key, String value, Context<String, Long> context) {
                context.emit(value, ONE);
            }

    }
