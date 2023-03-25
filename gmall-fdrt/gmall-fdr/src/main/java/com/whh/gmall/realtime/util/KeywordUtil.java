package com.whh.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: KeywordUtil
 * Package: com.whh.gmall.realtime.util
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 15:41
 * @Version 1.0
 */
public class KeywordUtil {
    public static List<String> analyze(String text){

        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,true);

        try {
            Lexeme lexeme = null;
            while((lexeme = ikSegmenter.next())!=null){
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return keywordList;
    }

    public static void main(String[] args) {
        List<String> list = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(list);
    }
}
