package streaming;

import beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsTableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("D:\\software\\IntelliJ IDEA 2020.2.1\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> filterInputUserBehavior = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new UserBehavior(new Long(fileds[0]), new Long(fileds[1]), new Integer(fileds[2]), fileds[3], new Long(fileds[4]));
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                })
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()));

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env,settings);

        Table dataTable = tenv.fromDataStream(filterInputUserBehavior, "itemId,timestamp.rowtime as ts");

        Table windowAggTable = dataTable.window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");
        DataStream<Row> aggStream = tenv.toAppendStream(windowAggTable, Row.class);
        tenv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");

        Table resultTable = tenv.sqlQuery("select * from " +
                "(select *,row_number() over(" +
                "partition by windowEnd order by cnt desc) as row_num " +
                "from agg) " +
                "where row_num <= 5");

//        tenv.toAppendStream(resultTable,Row.class).print();
        tenv.toRetractStream(resultTable,Row.class).print();
        env.execute("HotItemTableAPI");
    }
}
