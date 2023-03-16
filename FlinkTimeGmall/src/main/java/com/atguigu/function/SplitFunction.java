package com.atguigu.function;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author MengX
 * @create 2023/3/15 11:31:44
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {

        List<String> words = null;

        try {
            words = KeywordUtil.splitWords(str);
            for (String word : words) {

                collect(Row.of(word));
            }
        } catch (IOException e) {

            collect(Row.of(str));
        }

    }
}