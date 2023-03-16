package com.atguigu.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.function.DwdBroadcastProcessFunction;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author MengX
 * @create 2023/3/13 15:09:29
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        //todo 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        //设置HDFS用户信息
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //todo 从kafka获取主流数据 并转换主流数据为JSONObject类型
        String topic = "topic_db";
        String groupId = "dwdTokafka";
        SingleOutputStreamOperator<JSONObject> flatDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId))
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                        if (value != null && "".equals(value)) {

                            try {

                                JSONObject valueJSON = JSON.parseObject(value);

                                out.collect(valueJSON);
                            } catch (Exception e) {

                                System.out.println("脏数据：" + value);
                            }
                        } else
                            System.out.println("数据源没有数据");
                    }
                });

        //todo 从mysql获取配置流

        MySqlSource<String> mysqlS = MySqlSource.<String>builder()
                .hostname("hadoop102:9092")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlDS
                = env.fromSource(mysqlS, WatermarkStrategy.noWatermarks(), "mysql");

        //todo 配置流转换成广播流
        MapStateDescriptor<String, TableProcess> broadState
                = new MapStateDescriptor<>("broadTble", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDs
                = mysqlDS.broadcast(broadState);

        //todo 链接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = flatDs.connect(broadcastDs);

        //todo 处理链接流
        SingleOutputStreamOperator<JSONObject> processDs
                = connectedStream.process(new DwdBroadcastProcessFunction(broadState));

        //todo 把数据写入到kafka

        processDs.addSink(MyKafkaUtil.getDWDKafkaProduce(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {

                String sinkTable = element.getString("sinkTable");
                element.remove("sinkTable");
                return new ProducerRecord<byte[], byte[]>(sinkTable,element.toString().getBytes());
            }
        }));

        env.execute("BaseDBApp");

    }



}