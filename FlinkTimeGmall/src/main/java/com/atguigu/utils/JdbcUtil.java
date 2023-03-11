package com.atguigu.utils;


import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;


/**
 * @author MengX
 * @create 2023/3/10 08:47:39
 */

public class JdbcUtil {


    /**
     * select count(*) from t;                       单行单列
     * select * from t where id='xx';id为主键         单行多列
     * select count(*) from t group by tm_name;      多行单列
     * select * from t;                              多行多列
     */

    public static <T>List<T> queryList(Connection connection,
                                       String sql, Class<T> clz, boolean isUnderScoreToCamel) throws Exception {


        ArrayList<T> list = new ArrayList<>();


        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());

        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){

            T t = clz.newInstance();

            for (int i = 0; i < columnCount ; i++) {

                String columnName = metaData.getColumnName(i + 1);

                Object value = resultSet.getObject(columnName);

                if (isUnderScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //数据写入到T里面

                BeanUtils.setProperty(t, columnName, value);

            }

            list.add(t);

        }

        return list;

    }

}
