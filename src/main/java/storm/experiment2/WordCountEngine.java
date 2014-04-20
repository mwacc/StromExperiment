package storm.experiment2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.experiment2.bolts.CountBolt;
import storm.experiment2.spouts.SocketReaderSpout;

/**
 * Created by kostya on 4/18/14.
 */
public class WordCountEngine {

    public static void  main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        System.out.println("Start running Storm");

        if( args.length == 0 )
            new WordCountEngine().runStormLocaly();
        else
            new WordCountEngine().runStorm( Integer.parseInt(args[0]) );
    }


    public void runStormLocaly() {
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("socketReader", new SocketReaderSpout(), 2);
        builder.setBolt("logger", new CountBolt(), 20).shuffleGrouping("socketReader");

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("socket-reader", conf, builder.createTopology());

    }

    public void runStorm(int maxTaskPar) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("socketReader", new SocketReaderSpout(), 1);
        builder.setBolt("logger", new CountBolt(), 20).shuffleGrouping("socketReader");

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setMaxTaskParallelism(maxTaskPar);
        StormSubmitter.submitTopology("socket-reader", conf, builder.createTopology());
    }

}
