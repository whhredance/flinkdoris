package com.whh.gmall.realtime.app.dws;

import com.whh.gmall.realtime.app.func.KeywordUDTF;
import com.whh.gmall.realtime.app.func.MyDorisSink;
import com.whh.gmall.realtime.bean.KeywordBean;
import com.whh.gmall.realtime.common.GmallConstant;
import com.whh.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.whh.gmall.realtime.app.dws
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 15:59
 * @Version 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO 2. 检查点设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1L), Time.minutes(1L)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_traffic_page_log 主题中读取页面浏览日志数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page` map<string, string>,\n" +
                "`ts` bigint,\n" +
                "row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupId));

        // TODO 4. 从表中过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] full_word,\n" +
                "row_time\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("search_table", searchTable);

        // TODO 5. 使用自定义的UDTF函数对搜索的内容进行分词
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "row_time \n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(full_word))\n" +
                "as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 6. 分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyyMMdd') cur_date," +
                "count(*) keyword_count\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");

        // TODO 7. 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(KeywordBeanSearch, KeywordBean.class);

        // TODO 8. 将流中的数据写到ClickHouse中
        String tableName = "dws_traffic_source_keyword_page_view_window";
        keywordBeanDS.addSink(new MyDorisSink<KeywordBean>(tableName, "cur_date"));

        env.execute();
    }
}
