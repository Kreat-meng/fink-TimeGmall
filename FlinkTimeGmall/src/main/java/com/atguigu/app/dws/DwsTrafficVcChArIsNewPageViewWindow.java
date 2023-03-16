package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.utils.ClickhoseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author MengX
 * @create 2023/3/14 16:12:02
 */
public class DwsTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //todo 开启检查点

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        //设置HDFS用户信息
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //todo 从kafka获取用户行为数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_channel_page_view_window1";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        //todo 转换数据为json 格式，并按照mid 分组
        SingleOutputStreamOperator<JSONObject> mapDs = kafkaDs.map(line -> JSON.parseObject(line));

        KeyedStream<JSONObject, String> keyedDs = mapDs.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //todo 把分组后的数据封装成javabean格式
        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = keyedDs.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {

            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);

                //todo 设置状态ttl为1天，更新方式为写入和创建；
                StateTtlConfig ttlConfig = new StateTtlConfig
                        .Builder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stringValueStateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stringValueStateDescriptor);

            }

            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {

                String curState = valueState.value();
                String ts = DateFormatUtil.toDate(value.getLong("ts"));
                Long uv = 0L;
                if (curState == null || !curState.equals(ts)) {
                    uv = 1L;
                    valueState.update(ts);
                }
                Long sv = 0L;
                if (value.getJSONObject("page").getString("last_page") == null) {

                    sv = 1L;
                }
                Long duringTime = value.getJSONObject("page").getLong("during_time");

                JSONObject common = value.getJSONObject("common");

                String ar = common.getString("ar");

                String vc = common.getString("vc");

                String ch = common.getString("ch");

                String isNew = common.getString("is_new");

                return new TrafficPageViewBean("", "", vc, ch, ar, isNew, uv, sv, 1L, duringTime, value.getLong("ts"));

            }
        });

        //todo 获取wotermark
        SingleOutputStreamOperator<TrafficPageViewBean> watermarkDs = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        //分组开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDs = watermarkDs.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIsNew());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //todo 分组聚合
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowDs.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());


                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {

                TrafficPageViewBean next = input.iterator().next();

                long start = window.getStart();
                long end = window.getEnd();
                String stt = DateFormatUtil.toYmdHms(start);
                String edt = DateFormatUtil.toYmdHms(end);
                next.setStt(stt);
                next.setEdt(edt);
                next.setTs(System.currentTimeMillis());

                //输出数据
                out.collect(next);
            }
        });

        resultDS.print(">>>>>>>>>>>>>>>>>>>>>>>>");

        resultDS.addSink(ClickhoseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute("DwsTrafficVcChArIsNewPageViewWindow");


    }
}
