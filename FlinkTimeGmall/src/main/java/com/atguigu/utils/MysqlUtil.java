package com.atguigu.utils;

/**
 * @author MengX
 * @create 2023/3/16 15:02:17
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL(){

        return "create table base_dic (\n" +
                "`dic_code` STRING,\n" +
                "`dic_name` STRING,\n" +
                "`parent_code` STRING,\n" +
                "`create_time` STRING,\n" +
                "`operate_time` STRING\n" +
                ")" +mysqlLookUpTableDDL("base_dic");


    }

    public static String mysqlLookUpTableDDL(String tableName){

        return "" +
                " WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',\n" +
                "  'table-name' = '"+tableName+"',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'lookup.cache.max-rows' = '10',\n" +
                "  'lookup.cache.ttl' = '1 hour',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";

    }
}
