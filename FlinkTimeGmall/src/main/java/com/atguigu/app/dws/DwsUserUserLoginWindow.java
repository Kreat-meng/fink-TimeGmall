package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.utils.ClickhoseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @create 2023/3/15 20:54:56
 */
public class DwsUserUserLoginWindow {

    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 2. 状态后端设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        //设置HDFS用户信息
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // TODO 3. 读取页面数据封装为流 并封装成JSON 过滤数据，只保留用户 id 不为 null 且 last_page_id 为 null 或为 login 的数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonFlatDs = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageID = jsonObject.getJSONObject("page").getString("last_page_id");

                if (uid != null && (lastPageID == null || lastPageID.equals("login"))) {

                    out.collect(jsonObject);
                }
            }
        });
        //获取watermark
        SingleOutputStreamOperator<JSONObject> watermarkDs
                = jsonFlatDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // TODO 按uid分组，分别计算七日回流（回归）用户和当日独立用户数，并把数据封装在JAVAbean中
        SingleOutputStreamOperator<UserLoginBean> userLoginBeanDS = watermarkDs.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        }).flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastDL", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {

                Long uuct = 0L;
                Long backct = 0L;
                String lastDt = valueState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                Long lats = DateFormatUtil.toTs(lastDt);

                if (lastDt == null) {

                    uuct = 1L;
                    valueState.update(curDt);

                } else if (!lastDt.equals(curDt)) {

                    uuct = 1L;
                    valueState.update(curDt);

                    if ((ts - lats) / (24 * 3600 * 1000) > 7) {

                        backct = 1L;
                    }

                }
                if (uuct == 1L) {

                    out.collect(new UserLoginBean("", "", backct, uuct, null));
                }

            }

        });
        //开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userLoginBeanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {

                value1.setBackCt(value1.getBackCt() + value2.getBackCt());

                value1.setUuCt(value1.getUuCt() + value2.getUuCt());

                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {

                long start = window.getStart();
                String stt = DateFormatUtil.toDate(start);
                long end = window.getEnd();
                String edt = DateFormatUtil.toDate(end);
                Long ts = System.currentTimeMillis();

                UserLoginBean next = values.iterator().next();

                next.setStt(stt);
                next.setEdt(edt);
                next.setTs(ts);

                out.collect(next);
            }
        });

        resultDS.print(">>>>>>>>>>>>>>>>>>>>>>>>>");

        resultDS.addSink(ClickhoseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        env.execute("DwsUserUserLoginWindow");
    }
}
