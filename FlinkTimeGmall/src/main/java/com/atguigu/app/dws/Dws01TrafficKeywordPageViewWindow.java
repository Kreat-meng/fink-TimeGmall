package com.atguigu.app.dws;

import com.atguigu.bean.KeywordBean;
import com.atguigu.function.SplitFunction;
import com.atguigu.utils.ClickhoseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author MengX
 * @create 2023/3/14 16:08:00
 */
public class Dws01TrafficKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkcdc/220926");

        //设置HDFS用户信息
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //todo 从kafka读取页面浏览数据,并生成watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "keyword_220926";
        tableEnv.executeSql("" +
                "create table page_log (\n" +
                "    `common` MAP<STRING,STRING>,\n" +
                "    `page` MAP<STRING,STRING>,\n" +
                "    `ts` BIGINT,\n" +
                "    `rt` AS TO_TIMESTAMP_LTZ(ts,3),\n" +
                "     WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ")"+ MyKafkaUtil.getKafkaDDL(topic,groupId));
        //过滤数据
        Table flitTable = tableEnv.sqlQuery("" +
                "select \n" +
                "    `page`['item'] item,\n" +
                "    `rt`\n" +
                "from page_log\n" +
                "where `page`['last_page_id'] = 'search'\n" +
                "and `page`['item_type'] = 'keyword'\n" +
                "and `page`['item'] is not null ");

        tableEnv.createTemporaryView("flit_table",flitTable);

        //分词
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);

        //todo 对与item字段且词
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT \n" +
                "      rt, \n" +
                "      word\n" +
                "FROM \n" +
                "      flit_table, \n" +
                "LATERAL TABLE(SplitFunction(item))");

        tableEnv.createTemporaryView("split_table",splitTable);

        //todo 分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "SELECT \n" +
                "    date_format(window_start,'yyyy-MM-dd HH:mm:ss') stt, \n" +
                "    date_format(window_end,'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    word keyword,\n" +
                "    count(*) keyword_count,\n" +
                "    UNIX_TIMESTAMP() ts\n" +
                "FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(rt), INTERVAL '10' SECONDS))\n" +
                "GROUP BY word, window_start, window_end");

        //todo 将表转换为流 写入 clickhouse
        DataStream<KeywordBean> keywordBeanDs = tableEnv.toAppendStream(resultTable, KeywordBean.class);

        keywordBeanDs.print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        keywordBeanDs.addSink(ClickhoseUtil.getSinkFunction("insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)"));

        env.execute("Dws01TrafficKeywordPageViewWindow");


    }
}
