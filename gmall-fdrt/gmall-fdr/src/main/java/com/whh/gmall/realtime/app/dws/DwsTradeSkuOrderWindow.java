package com.whh.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.whh.gmall.realtime.app.func.DimAsyncFunction;
import com.whh.gmall.realtime.app.func.MyDorisSink;
import com.whh.gmall.realtime.bean.TradeSkuOrderBean;
import com.whh.gmall.realtime.util.DateFormatUtil;
import com.whh.gmall.realtime.util.KafkaUtil;
import com.whh.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeSkuOrderWindow
 * Package: com.whh.gmall.realtime.app.dws
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 17:15
 * @Version 1.0
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
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

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_sku_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤字段不完整数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        return userId != null && sourceTypeName != null;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = this.lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeSkuOrderBean> javaBeanStream = processedStream.map(
                jsonObj -> {
                    String skuId = jsonObj.getString("sku_id");
                    BigDecimal splitOriginalAmount = new BigDecimal(
                            jsonObj.getString("split_original_amount") == null ? "0.0" :
                                    jsonObj.getString("split_original_amount"));
                    BigDecimal splitActivityAmount = new BigDecimal(
                            jsonObj.getString("split_activity_amount") == null ? "0.0" :
                                    jsonObj.getString("split_activity_amount"));
                    BigDecimal splitCouponAmount = new BigDecimal(
                            jsonObj.getString("split_coupon_amount") == null ? "0.0" :
                                    jsonObj.getString("split_coupon_amount"));
                    BigDecimal splitTotalAmount = new BigDecimal(
                            jsonObj.getString("split_total_amount") == null ? "0.0" :
                                    jsonObj.getString("split_total_amount"));
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    TradeSkuOrderBean trademarkCategoryUserOrderBean = TradeSkuOrderBean.builder()
                            .skuId(skuId)
                            .originalAmount(splitOriginalAmount)
                            .activityAmount(splitActivityAmount)
                            .couponAmount(splitCouponAmount)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );

        // TODO 8. 设置水位线
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = javaBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {

                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 9. 分组
        KeyedStream<TradeSkuOrderBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeSkuOrderBean, String>() {

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) throws Exception {
                        return javaBean.getSkuId();
                    }
                }
        );

        // TODO 10. 开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 11. 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reducedStream = windowDS
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {

                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                String curDate = DateFormatUtil.toPartitionDate(context.window().getStart());
                                for (TradeSkuOrderBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setCurDate(curDate);
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 12. 维度关联，补充与分组无关的维度字段
        // 12.1 关联 sku_info 表
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info".toUpperCase()) {

                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setSkuName(jsonObj.getString("sku_name".toUpperCase()));
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );
        // 12.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(
                withSkuInfoStream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 12.3 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 12.4 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 12.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 12.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // TODO 13. 写出到 OLAP 数据库
        String tableName = "dws_trade_sku_order_window";
        withCategory1Stream.addSink(new MyDorisSink<TradeSkuOrderBean>(tableName));

        env.execute();
    }
}
