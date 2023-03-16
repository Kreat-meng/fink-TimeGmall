package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author MengX
 * @create 2023/3/14 19:12:25
 */
public class ClickhoseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {


        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        Class<?> aClass = t.getClass();

                        Field[] fields = aClass.getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];

                            field.setAccessible(true);

                            //todo 尝试获取 注解 transientsink 标记的字段，如果有则跳过
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {

                                offset++;
                                continue;
                            }

                            Object value = field.get(t);

                            preparedStatement.setObject(i + 1 - offset, value);

                        }


                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());

    }
}
