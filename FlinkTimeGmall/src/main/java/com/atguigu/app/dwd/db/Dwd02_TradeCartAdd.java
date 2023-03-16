package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author MengX
 * @create 2023/3/11 16:36:32
 */
public class Dwd02_TradeCartAdd {

    public static void main(String[] args) {

        //TODO 获取表和流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用flinksql的方法获取kafka数据
        String topic = "topic_db";
        String groupId = "dwd_trade_cart_add";
        tableEnv.executeSql(MyKafkaUtil.getTopicDBDDL(topic,groupId));

        Table cartInfo = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['cart_price'] cart_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['is_checked'] is_checked,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['is_ordered'] is_ordered,\n" +
                "    `data`['order_time'] order_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id,\n" +
                "    pt\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'cart_info'\n" +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("cart_info",cartInfo);

        tableEnv.sqlQuery("select * from cart_info").execute().print();

        //todo 创建lookup表basedic表与主表cartinfo进行jion
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //lookup join 注意 关键的join语句 TODO "join base_dic FOR SYSTEM_TIME AS OF cart.pt AS dic"
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    cart.id,\n" +
                "    cart.user_id,\n" +
                "    cart.sku_id,\n" +
                "    cart.cart_price,\n" +
                "    cart.sku_num,\n" +
                "    cart.sku_name,\n" +
                "    cart.is_checked,\n" +
                "    cart.create_time,\n" +
                "    cart.operate_time,\n" +
                "    cart.is_ordered,\n" +
                "    cart.order_time,\n" +
                "    cart.source_type,\n" +
                "    dic.dic_name,\n" +
                "    cart.source_id\n" +
                "from cart_info cart\n" +
                "join base_dic FOR SYSTEM_TIME AS OF cart.pt AS dic\n" +
                "on cart.source_type=dic.dic_code");
        tableEnv.createTemporaryView("result_table",resultTable);

        // todo 创建kafka加购表，准备数据写入

        tableEnv.executeSql("" +
                "create table dwd_cart_info(\n" +
                "    `id` STRING,\n" +
                "    `user_id` STRING,\n" +
                "    `sku_id` STRING,\n" +
                "    `cart_price` STRING,\n" +
                "    `sku_num` STRING,\n" +
                "    `sku_name` STRING,\n" +
                "    `is_checked` STRING,\n" +
                "    `create_time` STRING,\n" +
                "    `operate_time` STRING,\n" +
                "    `is_ordered` STRING,\n" +
                "    `order_time` STRING,\n" +
                "    `source_type` STRING,\n" +
                "    `dic_name` STRING,\n" +
                "    `source_id` STRING\n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));
        //数据写入
        tableEnv.executeSql("insert into dwd_cart_info select * from result_table");

    }
}
