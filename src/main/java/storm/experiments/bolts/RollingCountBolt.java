package storm.experiments.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.experiments.utils.SlidingWindow;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RollingCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;

    private final SlidingWindow counter;

    private OutputCollector collector;

    public RollingCountBolt(int windowLengthInSeconds, int maxObjectsInWindow) {
        counter = new SlidingWindow(windowLengthInSeconds * 1000, maxObjectsInWindow);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
       /* if( counter.isReady() ) {
            for(Map.Entry<SlidingWindow.UniqueKey, AtomicInteger> e : counter.getContent().entrySet()) {
                System.out.println( String.format("RollingCountBolt emit: [%s, %s, %d]", e.getKey().getCountry(), e.getKey().getUrl(), e.getValue().get()) );
                collector.emit( new Values(e.getKey().getCountry(), e.getKey().getUrl(), e.getValue().get()) );
            }
            counter.reset();
        }
        counter.emitTuple(tuple);
        collector.ack(tuple);   */
        System.out.println("Inside fist BOLT");

        collector.emit(tuple, Arrays.asList( new Object[]{"Test1", "TEst2", "Test3"} ));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country", "url", "eventsNum"));
    }
}