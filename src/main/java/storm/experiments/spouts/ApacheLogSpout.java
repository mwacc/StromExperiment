package storm.experiments.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.File;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ApacheLogSpout extends BaseRichSpout {

    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    private final File logFile;
    private SpoutOutputCollector collector;


    public ApacheLogSpout(String logFile) {
        this.logFile = new File(logFile);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country", "url"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try {
            TailerListener listener = new LogTailListener();
            Tailer tailer = new Tailer(logFile, listener, 500);

            Thread thread = new Thread(tailer);
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String logRow = queue.poll();
        if( logRow == null ) {
            Utils.sleep(50);
        } else {
            String[] parameters = logRow.split(",");
            collector.emit( new Values(parameters[1], parameters[2]));
        }
    }

    class LogTailListener extends TailerListenerAdapter {
        public void handle(String line) {
            try {
                queue.put(line);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}