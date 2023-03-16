package com.atguigu.utils;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author MengX
 * @create 2023/3/8 19:48:27
 */
public class MyKafkaUtil {

    private static final String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop102:9092";

    private static String defuftTopic = "dwdToKafka";

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){

        Properties props= new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                if (record == null || record.value() == null) {

                    return null;
                } else{

                    return new String(record.value());

                }
            }

            @Override
            public TypeInformation<String> getProducedType() {

                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        },props);

        return flinkKafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducerFunction(String topic){

        return new FlinkKafkaProducer<String>(
                BOOTSTRAP_SERVERS,topic,new SimpleStringSchema());

    }

    public static <T>FlinkKafkaProducer<T> getDWDKafkaProduce(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        return new FlinkKafkaProducer<T>(defuftTopic,kafkaSerializationSchema,properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //可以看作kafka消费者
    public static String getKafkaDDL(String topic,String groupId){

        return  "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVERS+"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
    //kafka生产者
    public static String getKafkaSinkDDL(String topic){

        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVERS+"',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic){

        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVERS+"',\n" +
                "  'key.format' = 'avro',\n" +
                "  'value.format' = 'avro'\n" +
                ")";
    }

    public static String getTopicDBDDL(String topic, String groupId){

        return "" +
                "create table topic_db (\n" +
                "    `database` STRING,\n" +
                "    `table` STRING,\n" +
                "    `type` STRING,\n" +
                "    `ts` BIGINT,\n" +
                "    `data` MAP<STRING,STRING>,\n" +
                "    `old` MAP<STRING,STRING>,\n" +
                "    `pt` AS PROCTIME()\n" +
                ")" + getKafkaDDL(topic,groupId);
    }
}
