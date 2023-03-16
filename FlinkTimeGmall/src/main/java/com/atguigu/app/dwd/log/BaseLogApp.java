package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigInteger;

/**
 * @author MengX
 * @create 2023/3/8 16:52:32
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        System.setProperty("HADOOP_USER_NAME","atguigu");

        //获取数据并把数据转化为json对象
        String topic = "topic_log";
        String groupId = "dwdConsunmer";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsDs = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

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
        // 数据按照mid分组
        KeyedStream<JSONObject, String> keyDs = jsDs.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {

                String mid = value.getJSONObject("common").getString("mid");

                return mid;
            }
        });

        //做新老数据的校验
        SingleOutputStreamOperator<JSONObject> mapDs
                = keyDs.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String>  valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                valueState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("is_new",String.class));

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                String is_new = value.getJSONObject("common").getString("is_new");
                String cur = valueState.value();
                BigInteger ts = value.getBigInteger("ts");
                String s = DateFormatUtil.toDate(ts.longValue());
                if (is_new.equals("1")) {

                    if (cur == null || cur.equals(s) ) {

                        valueState.update(s);
                    }else {

                        value.getJSONObject("common").put("is_new","0");
                    }
                }else {

                    if (cur == null)

                    valueState.update("1970-01-01");
                }

                return value;
            }
        });

        //创建测输出流 主流:页面  侧输出流:启动、曝光、动作、错误
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> dispayTag = new OutputTag<String>("display") {};
        OutputTag<String> actionTag = new OutputTag<String>("action") {};
        OutputTag<String> errorTag = new OutputTag<String>("error") {};

        //获取侧输出流把数据分流
        SingleOutputStreamOperator<String> pageDs = mapDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value,
                                       ProcessFunction<JSONObject, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                //共生:错误----启动/页面
                //互斥:启动、页面
                //包含:页面--->曝光、动作

                //尝试获取错误信息流
                String err = value.getString("err");
                if (err != null ){

                    ctx.output(errorTag,value.toString());

                    value.remove("err");
                }
                //尝试获取启动日志流
                String start = value.getString("start");

                if (start != null){

                    ctx.output(startTag,value.toString());
                }else {

                    //获取页面ID以及时间戳
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    //输出曝光流
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null){

                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);

                            display.put("page_id",pageId);

                            display.put("ts",ts);

                            ctx.output(dispayTag,display.toString());
                        }
                    }
                    //输出动作流
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null){

                        for (int i = 0; i < actions.size(); i++) {

                            JSONObject action = actions.getJSONObject(i);

                            action.put("page_id",pageId);

                            ctx.output(actionTag,action.toString());
                        }
                    }

                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toString());
                }

            }
        });

        //通过测输出流重新写入到kafka
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDs.print("page_topic");
        pageDs.getSideOutput(errorTag).print("errorTag");
        pageDs.getSideOutput(startTag).print("startTag");
        pageDs.getSideOutput(dispayTag).print("dispayTag");
        pageDs.getSideOutput(actionTag).print("actionTag");


        pageDs.addSink(MyKafkaUtil.getKafkaProducerFunction(page_topic));
        pageDs.getSideOutput(errorTag).addSink(MyKafkaUtil.getKafkaProducerFunction(error_topic));
        pageDs.getSideOutput(startTag).addSink(MyKafkaUtil.getKafkaProducerFunction(start_topic));
        pageDs.getSideOutput(dispayTag).addSink(MyKafkaUtil.getKafkaProducerFunction(display_topic));
        pageDs.getSideOutput(actionTag).addSink(MyKafkaUtil.getKafkaProducerFunction(action_topic));


        //执行
        env.execute("BaseLogApp");
    }
}
