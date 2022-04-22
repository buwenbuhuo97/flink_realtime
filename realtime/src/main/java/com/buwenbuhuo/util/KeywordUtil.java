package com.buwenbuhuo.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: IK 分词工具类 KeywordUtil
 */
public class KeywordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {

        // 创建集合用于存放结果数据
        ArrayList<String> result = new ArrayList<>();

        // 创建分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        // 提取分词
        Lexeme next = ikSegmenter.next();

        while (next != null){
            String word = next.getLexemeText();
            result.add(word);

            next = ikSegmenter.next();
        }

        // 返回结果
        return result;
    }

    public static void main(String[] args) throws IOException {
        // 测试
        List<String> list = splitKeyWord("我是大帅哥");
        /**
         * 测试举例：
         *      smart结果：     [我, 是, 大帅, 帅哥]
         *      max_word结果：  [我, 是, 大, 帅哥]
         * 对比选择使用max_word做分词统计
         */
        System.out.println(list);
    }
}
