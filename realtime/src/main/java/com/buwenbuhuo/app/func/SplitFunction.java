package com.buwenbuhuo.app.func;

import com.buwenbuhuo.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:38
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:用户自定义函数 KeywordUDTF
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public  class SplitFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        List<String> list;
        try {
            list = KeywordUtil.splitKeyWord(keyword);
            for (String word : list) {
                collect(Row.of(word));
            }
        }catch (IOException e){
            collect(Row.of(keyword));
        }
    }
}
