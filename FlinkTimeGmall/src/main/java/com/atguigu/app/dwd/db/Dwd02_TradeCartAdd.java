package com.atguigu.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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







    }
}
