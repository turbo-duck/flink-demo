package icu.wzk.demo05;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class StartApp {

    private static final String KAFKA_SERVER = "0.0.0.0:9092";

    private static final Integer KAFKA_PORT = 9092;

    private static final String KAFKA_TOPIC = "test";

    private static final String REDIS_SERVER = "0.0.0.0";

    private static final Integer REDIS_PORT = 6379;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", String.format("%s:%d", KAFKA_SERVER, KAFKA_PORT));
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KAFKA_TOPIC, new SimpleStringSchema(), properties);
        DataStreamSource<String> data = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple2<String, String>> wordData = data.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("l_words", value);
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder()
                .setHost(REDIS_SERVER)
                .setPort(REDIS_PORT)
                .build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());
        wordData.addSink(redisSink);
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String,String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String,String> data) {
            return data.f1;
        }
    }

}
