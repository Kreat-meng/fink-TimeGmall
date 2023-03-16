package com.atguigu.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.sound.midi.MidiFileFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author MengX
 * @create 2023/3/13 16:12:11
 */
public class DwdBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;

    public DwdBroadcastProcessFunction(MapStateDescriptor<String,TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {


    }

    //value: FlinkCDC：
    //{"before":null,"after":{"source_table":"base_category3","sink_table":"dim_base_category3","sink_columns":"id,name,category2_id","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1669162876406,"snapshot":"false","db":"gmall-220623-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1669162876406,"transaction":null}
    @Override
    public void processBroadcastElement(String value,
                                        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        //todo 获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //todo 转换配置流数据并写入状态
        JSONObject valueJson = JSON.parseObject(value);
        String op = valueJson.getString("op");

        if (value != null){

            if ("d".equals(op)){

                TableProcess before = JSON.parseObject(valueJson.getString("before"), TableProcess.class);

                String key = before.getSourceTable() +"_"+ before.getSourceType();

                if ("dwd".equals(before.getSinkType())){

                    broadcastState.remove(key);
                }


            }else {

                TableProcess after = JSON.parseObject(valueJson.getString("after"), TableProcess.class);

                String key = after.getSourceTable()+"_"+after.getSourceType();

                if ("dwd".equals(after.getSinkType())){

                    broadcastState.put(key,after);
                }
            }

        }

    }


    @Override
    public void processElement(JSONObject value,
                               BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {

     //Maxwell-topic_db
     //{"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}

        //todo 获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String key = value.getString("table")+"_"+value.getString("type");

        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null){

            JSONObject data = value.getJSONObject("data");

            columnsflat(data,tableProcess);

            String sinkTable = tableProcess.getSinkTable();

            data.put("sinkTable",sinkTable);

            out.collect(data);

        }

    }

    public static void columnsflat(JSONObject jsonObject,TableProcess tableProcess) {

        String sinkColumns = tableProcess.getSinkColumns();

        String[] split1 = sinkColumns.split(",");

        List<String> columns = Arrays.asList(split1);

        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();

        while (iterator.hasNext()){

            String key = iterator.next().getKey();

            if (!columns.contains(key)){

                iterator.remove();
            }
        }

    }


}
