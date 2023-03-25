package com.whh.gmall.realtime.app.func;

import com.whh.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: KeywordUDTF
 * Package: com.whh.gmall.realtime.app.func
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 15:43
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text){
        for (String keyword: KeywordUtil.analyze(text)){
            collect(Row.of(keyword));
        }
    }

}
