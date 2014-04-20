package storm.experiment2.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kostya on 4/18/14.
 */
public class CountBolt extends BaseRichBolt {

    private String ip = "<undefined>";
    private String uuid = UUID.randomUUID().toString();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            System.out.println("\n Bolt is reary on host "
                    + InetAddress.getLocalHost().getHostName() + "\n\n");
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if( tuple != null) {
            System.out.print( String.format("\n Bolt %s got msg '%s' from host %s on host %s\n",
                    uuid, tuple.getString(1), tuple.getString(0), ip) );
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
