package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author MengX
 * @create 2023/3/15 11:11:09
 */
public class KeywordUtil {

    public static List<String> splitWords(String word) throws IOException {

        //创建结果集合
        ArrayList<String> words = new ArrayList<>();


            IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(word), false);

            Lexeme next = ikSegmenter.next();

            while (next != null){

                String keyWrod = next.getLexemeText();

                words.add(keyWrod);

                next = ikSegmenter.next();

            }


        return words;
    }

    public static void main(String[] args) throws IOException {

        System.out.println(splitWords("尚硅谷实时数仓").toString());
    }
}
