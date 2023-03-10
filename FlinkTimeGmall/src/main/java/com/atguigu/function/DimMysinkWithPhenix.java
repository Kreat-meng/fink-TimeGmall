package com.atguigu.function;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @author MengX
 * @create 2023/3/10 11:02:54
 */
public class DimMysinkWithPhenix extends RichSinkFunction<JSONObject> {


    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //获取 phenix 链接

        DruidPooledConnection phoenixConn = DruidDSUtil.getPhoenixConn();

        String sql = getInsertSql(value);
        //预先编译sql
        PreparedStatement preparedStatement = phoenixConn.prepareStatement(sql);

        preparedStatement.execute();

        phoenixConn.close();

    }

    private String getInsertSql(JSONObject value) {

        /**
         * Maxwell-topic_db
         * <p>
         * {"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,
         * "xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,
         * "head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204",
         * "comment_txt":"评论内容：48384811984748167197482849234338563286217912223261",
         * "create_time":"2022-08-02 08:22:38","operate_time":null}}
         */

        StringBuilder sql = new StringBuilder();

        // todo 获取插入数据的SQL语句:upsert into db.tn(id,name,sex) values('1001','zs','male)

        sql.append(" upsert into "+ GmallConfig.HBASE_SCHEMA+"."+value.getString("sinkTable"));

        value.remove("sinkTable");
        Set<java.lang.String> columns = value.keySet();

        Collection<Object> values = value.values();

        java.lang.String colum = StringUtils.join(columns, ",");

        java.lang.String va = StringUtils.join(values, "','");

        sql.append("( "+colum+")"
                +" values("+"'"+va+"')");

        System.out.println(sql);

        java.lang.String s = sql.toString();

        return s;

    }

}
