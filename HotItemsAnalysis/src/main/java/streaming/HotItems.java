package streaming;

import beans.ItemViewCount;
import beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class HotItems {
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

        SingleOutputStreamOperator<ItemViewCount> windowAggStream = filterInputUserBehavior.keyBy("itemId").timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new windowItemCountResule());

        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy("windowEnd")
                .process(new TipNHotItems(5));

        resultStream.print();

        env.execute("HotItem");
    }

    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }
    public static class windowItemCountResule implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    public static class TipNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String>{
        private Integer topSize;

        public TipNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        ListState<ItemViewCount> listState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-ccunt-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            listState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(listState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=========================").append("\n");
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n");

            for (int i = 0; i < Math.min(itemViewCounts.size(),topSize); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                stringBuilder.append("NO ").append(i + 1).append(":")
                .append("商品ID:").append(currentItemViewCount.getItemId())
                        .append("热门度：").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            stringBuilder.append("=========================");
            Thread.sleep(1000L);
            out.collect(stringBuilder.toString());
        }
    }
}
