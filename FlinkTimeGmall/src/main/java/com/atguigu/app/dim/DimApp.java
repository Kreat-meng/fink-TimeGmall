package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.function.DimMysinkWithPhenix;
import com.atguigu.function.DimTableProcessFunction;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/3/8 16:50:04
 */
public class DimApp {

    public static void main(String[] args) throws Exception {

        // 1.todo 创建流环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 配置检查点，生产环境必须配置
        env.enableCheckpointing(5000l, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000l);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc");
        //设置 hdfs 用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 2.todo 读取 kafka 主题 “ tapic_db” 数据得到主流
        String topic = "topic_db";

        String groupId = "dim_app";

        DataStreamSource<String> kafkaDbStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 2.1 todo 对主流数据进行过滤封装成Json对象
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaDbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                //存疑？
                if (value != null) {

                    try {

                        JSONObject jsonObject = JSON.parseObject(value);

                        out.collect(jsonObject);

                    } catch (Exception e) {

                        System.out.println("脏数据：" + value);
                    }

                }
            }
        });
        // 3。todo 读取 mysql 中配置表信息转换为 配置流
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlpz =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql");

        // 4. todo 转换 配置流为广播流
        MapStateDescriptor<String, TableProcess> braodstate = new MapStateDescriptor<>("braodstate", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = mysqlpz.broadcast(braodstate);

        // 5. todo 链接并处理两条流
        SingleOutputStreamOperator<JSONObject> habaseStream =
                jsonStream.connect(broadcastStream).process(new DimTableProcessFunction(braodstate));


        // 9. todo 主流数据写入 phinx 实现sink方法
        habaseStream.addSink(new DimMysinkWithPhenix());

        // 10. todo 执行操作
        env.execute("dimAPP");
    }
}