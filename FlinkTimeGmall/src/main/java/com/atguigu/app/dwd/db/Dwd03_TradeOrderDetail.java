package com.atguigu.app.dwd.db;


import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author MengX
 * @create 2023/3/16 16:30:34
 */
public class Dwd03_TradeOrderDetail {

    public static void main(String[] args) {

        //TODO 获取表和流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用flinksql的方法获取kafka数据
        String topic = "topic_db";
        String groupId = "dwd_trade_order_detail";
        tableEnv.executeSql(MyKafkaUtil.getTopicDBDDL(topic,groupId));

        //订单明细表
        Table orderDetail = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['img_url'] img_url,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "    pt\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail",orderDetail);

        //订单表
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['consignee'] consignee,\n" +
                "    `data`['consignee_tel'] consignee_tel,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['order_status'] order_status,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_way'] payment_way,\n" +
                "    `data`['delivery_address'] delivery_address,\n" +
                "    `data`['order_comment'] order_comment,\n" +
                "    `data`['out_trade_no'] out_trade_no,\n" +
                "    `data`['trade_body'] trade_body,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['expire_time'] expire_time,\n" +
                "    `data`['process_status'] process_status,\n" +
                "    `data`['tracking_no'] tracking_no,\n" +
                "    `data`['parent_order_id'] parent_order_id,\n" +
                "    `data`['img_url'] img_url,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "    `data`['original_total_amount'] original_total_amount,\n" +
                "    `data`['feight_fee'] feight_fee,\n" +
                "    `data`['feight_fee_reduce'] feight_fee_reduce,\n" +
                "    `data`['refundable_time'] refundable_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_info",orderInfoTable);

        // 订单活动表
        Table detailTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_activity",detailTable);

        //订单优惠券 表
        Table couponTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id'] coupon_id,\n" +
                "    `data`['coupon_use_id'] coupon_use_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_coupon",couponTable);

        //获取basedic的lookup表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        Table resultTale = tableEnv.sqlQuery("" +
                "select\n" +
                "    od.id,\n" +
                "    od.order_id,\n" +
                "    od.sku_id,\n" +
                "    od.sku_name,\n" +
                "    od.order_price,\n" +
                "    od.sku_num,\n" +
                "    od.create_time,\n" +
                "    od.source_type,\n" +
                "    dic.dic_name,\n" +
                "    od.source_id,\n" +
                "    od.split_total_amount,\n" +
                "    od.split_activity_amount,\n" +
                "    od.split_coupon_amount,\n" +
                "    oi.consignee,\n" +
                "    oi.consignee_tel,\n" +
                "    oi.total_amount,\n" +
                "    oi.order_status,\n" +
                "    oi.user_id,\n" +
                "    oi.payment_way,\n" +
                "    oi.delivery_address,\n" +
                "    oi.order_comment,\n" +
                "    oi.out_trade_no,\n" +
                "    oi.trade_body,\n" +
                "    oi.expire_time,\n" +
                "    oi.process_status,\n" +
                "    oi.tracking_no,\n" +
                "    oi.parent_order_id,\n" +
                "    oi.province_id,\n" +
                "    oi.activity_reduce_amount,\n" +
                "    oi.coupon_reduce_amount,\n" +
                "    oi.original_total_amount,\n" +
                "    oi.feight_fee,\n" +
                "    oi.feight_fee_reduce,\n" +
                "    oa.activity_id,\n" +
                "    oa.activity_rule_id,\n" +
                "    oc.coupon_id,\n" +
                "    oc.coupon_use_id\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_activity oa\n" +
                "on od.id = oa.order_detail_id\n" +
                "left join order_coupon oc\n" +
                "on od.id = oc.order_detail_id\n" +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic\n" +
                "on od.source_type = dic.dic_code");

        tableEnv.createTemporaryView("result_table",resultTale);

        tableEnv.executeSql("" +
                "create table dwd_order_detail(\n" +
                "    `id` STRING,\n" +
                "    `order_id` STRING,\n" +
                "    `sku_id` STRING,\n" +
                "    `sku_name` STRING,\n" +
                "    `order_price` STRING,\n" +
                "    `sku_num` STRING,\n" +
                "    `create_time` STRING,\n" +
                "    `source_type` STRING,\n" +
                "    `dic_name` STRING,\n" +
                "    `source_id` STRING,\n" +
                "    `split_total_amount` STRING,\n" +
                "    `split_activity_amount` STRING,\n" +
                "    `split_coupon_amount` STRING,\n" +
                "    `consignee` STRING,\n" +
                "    `consignee_tel` STRING,\n" +
                "    `total_amount` STRING,\n" +
                "    `order_status` STRING,\n" +
                "    `user_id` STRING,\n" +
                "    `payment_way` STRING,\n" +
                "    `delivery_address` STRING,\n" +
                "    `order_comment` STRING,\n" +
                "    `out_trade_no` STRING,\n" +
                "    `trade_body` STRING,\n" +
                "    `expire_time` STRING,\n" +
                "    `process_status` STRING,\n" +
                "    `tracking_no` STRING,\n" +
                "    `parent_order_id` STRING,\n" +
                "    `province_id` STRING,\n" +
                "    `activity_reduce_amount` STRING,\n" +
                "    `coupon_reduce_amount` STRING,\n" +
                "    `original_total_amount` STRING,\n" +
                "    `feight_fee` STRING,\n" +
                "    `feight_fee_reduce` STRING,\n" +
                "    `activity_id` STRING,\n" +
                "    `activity_rule_id` STRING,\n" +
                "    `coupon_id` STRING,\n" +
                "    `coupon_use_id` STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        //todo 数据写出
        tableEnv.executeSql("insert into dwd_order_detail select * from result_table");


    }
}
