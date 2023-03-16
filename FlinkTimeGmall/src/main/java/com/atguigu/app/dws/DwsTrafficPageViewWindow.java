package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.utils.ClickhoseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author MengX
 * @create 2023/3/15 20:54:23
 */
public class DwsTrafficPageViewWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        //设置HDFS用户信息
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 读取kafka数据转换为JSON
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId)).flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);

                String pageID = jsonObject.getJSONObject("page").getString("page_id");

                if (pageID.equals("home") || pageID.equals("good_detail")) {

                    out.collect(jsonObject);
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> wa = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        KeyedStream<JSONObject, String> keyDs = wa.keyBy((KeySelector<JSONObject, String>) value -> value.getJSONObject("common").getString("mid"));

        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = keyDs.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valuehomestate", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        String lastDt = valueState.value();
                        String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                        String pageID = value.getJSONObject("page").getString("page_id");

                        Long uvct = 0L;
                        Long goodUV = 0L;

                        if (pageID.equals("home")) {

                            if (lastDt == null || !lastDt.equals(curDt)) {
                                uvct = 1L;
                                valueState.update(curDt);
                            }
                        } else {

                            if (lastDt == null || !lastDt.equals(curDt)) {
                                goodUV = 1L;
                                valueState.update(curDt);
                            }
                        }
                        if (uvct == 1L || goodUV == 1L) {

                            out.collect(new TrafficHomeDetailPageViewBean("", "", uvct, goodUV, null));
                        }


                    }
                })
                //开窗聚合
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));


        //TODO 窗口聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDs
                = windowDS.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {

                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());

                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean next = values.iterator().next();

                String end = DateFormatUtil.toDate(window.getEnd());
                String start = DateFormatUtil.toDate(window.getStart());
                long ts = System.currentTimeMillis();
                next.setEdt(end);
                next.setTs(ts);
                next.setStt(start);

                out.collect(next);
            }
        });

        resultDs.print(">>>>>>>>>>>>>>>>>>>>>>>>>");
        resultDs.addSink(ClickhoseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        env.execute("DwsTrafficPageViewWindow");
    }

}
