package com.atguigu.function;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;

import java.util.*;

/**
 * @author MengX
 * @create 2023/3/9 10:22:45
 */
public class DimTableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> broadstate;

    private HashMap<String, TableProcess> stateHashMap;


    public DimTableProcessFunction(MapStateDescriptor<String, TableProcess> braadstate) {

        this.broadstate = braadstate;

    }

    // todo 防止 主流数据可能会在广播写入状态之前 到达处理函数，导致数据丢失，因此重写此方法预加载 状态 进行使用
    @Override
    public void open(Configuration parameters) throws Exception {

        stateHashMap = new HashMap<>();

        Connection connet = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?" + "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        List<TableProcess> tableProcessList = JdbcUtil.queryList(connet,
                "select * from table_process where sink_type = 'dim'", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcessList) {

            String key = tableProcess.getSourceTable();

            createtable(tableProcess);

            stateHashMap.put(key,tableProcess);
        }

    }

    @Override
    public void processBroadcastElement(String value,
                                        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx,
                                        Collector<JSONObject> out) throws Exception {


        //TODO 1. 获取广播状态 并转化广播流数据 写入状态

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(broadstate);

        //处理数据
        JSONObject jsonObject = JSON.parseObject(value);

        if ("d".equals(jsonObject.getString("op"))) {

            TableProcess ta = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);

            if ("dim".equals(ta.getSinkType())) {

                String key = ta.getSourceTable();

                broadcastState.remove(key);

                stateHashMap.remove(key);
            }

        } else {

            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

            if ("dim".equals(tableProcess.getSinkType())) {

                String key = tableProcess.getSourceTable();

                //TODO 2. 创建Phoenix dim表
                createtable(tableProcess);

                broadcastState.put(key, tableProcess);
            }

        }

    }

    //建表:create table if not exists db.tn
    // (id varchar primary key ,name varchar ,sex varchar) xxx
    private void createtable(TableProcess tableProcess) {

        //处理特殊字段
        String sinkPk = tableProcess.getSinkPk();

        String sinkExtend = tableProcess.getSinkExtend();

        if (sinkPk == null || sinkPk == "") {

            sinkPk = "id";
        }
        //不可用else if
        if (sinkExtend == null) {

            sinkExtend = "";
        }

        // 拼接sql
        StringBuilder sql = new StringBuilder("create table if not exists ");

        sql
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableProcess.getSinkTable())
                .append(" ( ");

        String sinkColumns = tableProcess.getSinkColumns();

        String[] split = sinkColumns.split(",");

        for (int i = 0; i < split.length; i++) {

            String flide = split[i];

            sql.append(flide).append(" varchar ");

            if (sinkPk.equals(flide)) {

                sql.append(" primary key ");

            }

            if (i < split.length - 1) {

                sql.append(" , ");

            } else

                sql.append(" ) ").append(sinkExtend);
        }

        System.out.println("建表语句：" + sql);

        //获取phoenix 链接
        try {
            DruidPooledConnection phoenixConn = DruidDSUtil.getPhoenixConn();
            //预编译 sql
            PreparedStatement preparedStatement = phoenixConn.prepareStatement(sql.toString());
            //执行sql
            preparedStatement.execute();

            phoenixConn.close();

        } catch (SQLException e) {

            e.printStackTrace();

            System.out.println("创建表失败: " + tableProcess.getSinkTable());

            throw new RuntimeException("创建表失败: " + tableProcess.getSinkTable());
        }


    }

    /**
     * Maxwell-topic_db
     * <p>
     * {"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,
     * "xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,
     * "head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204",
     * "comment_txt":"评论内容：48384811984748167197482849234338563286217912223261",
     * "create_time":"2022-08-02 08:22:38","operate_time":null}}
     */
    @Override
    public void processElement(JSONObject value,
                               BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {

        //获取状态

        ReadOnlyBroadcastState<String, TableProcess> broadcastState
                = ctx.getBroadcastState(broadstate);

        String type = value.getString("type");

        if (type.equals("insert") || type.equals("update") || type.equals("bootstrap-insert")) {

            TableProcess table = broadcastState.get(value.getString("table"));
            TableProcess tableMap = stateHashMap.get(value.getString("table"));

            if (table == null){

                table = tableMap;
            }

            if (table != null || tableMap != null) {

                JSONObject data1 = value.getJSONObject("data");

                //列过滤
                String sinkColumns = table.getSinkColumns();

                String[] split1 = sinkColumns.split(",");

                List<String> columns = Arrays.asList(split1);

                Iterator<Map.Entry<String, Object>> iterator = data1.entrySet().iterator();

                while (iterator.hasNext()){

                    String key = iterator.next().getKey();

                    if (!columns.contains(key)){

                        iterator.remove();
                    }
                }
                data1.put("sinkTable",table.getSinkTable());

                out.collect(data1);

            }


        }


    }

}
