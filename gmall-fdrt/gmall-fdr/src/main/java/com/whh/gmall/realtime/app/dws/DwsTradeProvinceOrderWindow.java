package com.whh.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.whh.gmall.realtime.app.func.DimAsyncFunction;
import com.whh.gmall.realtime.app.func.MyDorisSink;
import com.whh.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.whh.gmall.realtime.util.DateFormatUtil;
import com.whh.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeProvinceOrderWindow
 * Package: com.whh.gmall.realtime.app.dws
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 17:18
 * @Version 1.0
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka topic_db 主题读取业务数据
        String topic = "topic_db";
        String groupId = "dws_trade_province_order_window";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 筛选订单表数据
        SingleOutputStreamOperator<JSONObject> filteredStream = source.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getString("table");
                        String type = jsonObj.getString("type");
                        Long ts = jsonObj.getLong("ts");
                        if (table.equals("order_info") && type.equals("insert")) {
                            JSONObject data = jsonObj.getJSONObject("data");
                            data.put("ts", ts);
                            out.collect(data);
                        }
                    }
                }
        );

        // TODO 5. 转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderWindow> javaBeanStream = filteredStream.map(
                jsonObj -> {
                    String provinceId = jsonObj.getString("province_id");
                    BigDecimal orderAmount = new BigDecimal(jsonObj.getString("total_amount"));
                    Long ts = jsonObj.getLong("ts") * 1000L;

                    return TradeProvinceOrderWindow.builder()
                            .provinceId(provinceId)
                            .orderCount(1L)
                            .orderAmount(orderAmount)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderWindow> withWatermarkStream = javaBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderWindow>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderWindow javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 7. 按照省份 ID 分组
        KeyedStream<TradeProvinceOrderWindow, String> keyedByProIdStream =
                withWatermarkStream.keyBy(TradeProvinceOrderWindow::getProvinceId);

        // TODO 8. 开窗
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowDS = keyedByProIdStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
        ));

        // TODO 9. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TradeProvinceOrderWindow> elements, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        for (TradeProvinceOrderWindow element : elements) {
                            String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                            String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                            String curDate = DateFormatUtil.toPartitionDate(context.window().getStart());
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDate);
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 10. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> fullInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>("dim_base_province".toUpperCase()) {

                    @Override
                    public void join(TradeProvinceOrderWindow javaBean, JSONObject jsonObj) throws Exception {
                        String provinceName = jsonObj.getString("name".toUpperCase());
                        javaBean.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderWindow javaBean) {
                        return javaBean.getProvinceId();
                    }
                },
                60 * 50, TimeUnit.SECONDS
        );

        // TODO 11. 写入到 OLAP 数据库
        String tableName = "dws_trade_province_order_window";
        fullInfoStream.addSink(new MyDorisSink<TradeProvinceOrderWindow>(tableName));

        env.execute();
    }
}
