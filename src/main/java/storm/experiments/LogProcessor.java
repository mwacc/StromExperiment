package storm.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.experiments.bolts.MySqlBolt;
import storm.experiments.bolts.RollingCountBolt;
import storm.experiments.spouts.ApacheLogSpout;

import java.sql.SQLException;

public class LogProcessor {

    static {

    }

    public void performStorm() throws SQLException {
        TopologyBuilder builder = new TopologyBuilder();

        System.out.println("Start running Storm");

        builder.setSpout("logGenerator", new ApacheLogSpout("/tmp/storm/log.log"), 3);
        builder.setBolt("counter", new RollingCountBolt(5, 20), 4).fieldsGrouping("logGenerator", new Fields("url", "country"));
        builder.setBolt("mysql", new MySqlBolt(), 1).shuffleGrouping("counter");

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log-count", conf, builder.createTopology());

    }


    public static void main(String[] args) throws Exception {
        LogProcessor lp = new LogProcessor();
        lp.performStorm();
    }
}