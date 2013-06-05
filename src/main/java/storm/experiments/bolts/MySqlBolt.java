package storm.experiments.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.experiments.model.VisitorsFact;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

public class MySqlBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private transient Connection conn = null;
    private transient PreparedStatement preparedStatement;

    private OutputCollector collector;

    public MySqlBolt() {
        //initDb();
    }

    private void initDb() {
        if( preparedStatement != null ) return;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://10.25.9.245:3306/storm", "root", "root");
            preparedStatement = conn.prepareStatement("INSERT INTO VISITORS_STAT(country, url, visits) VALUES (?,?,?)" +
                    " ON DUPLICATE KEY UPDATE visits = visits + ? ");
            System.out.println("Connected to MySQL from instance " + this);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("Prepare second bolt");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
       /* if( tuple != null ) {
            try {
                System.out.println(String.format("Push in statistic queue, [%s, %s, %d]", tuple.getString(0), tuple.getString(1), tuple.getInteger(2)));
                save(new VisitorsFact(tuple.getString(1), tuple.getString(0), tuple.getInteger(2)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }  */

        System.out.println("Inside second BOLT");
        collector.fail(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    private void save(VisitorsFact visitorsFact) {
        initDb();
        System.out.println(String.format("Save into DB: [%s, %s, %d]", visitorsFact.getCountry(), visitorsFact.getUrl(), visitorsFact.getVisitors()));
        try {
            preparedStatement.setString(1, visitorsFact.getCountry());
            preparedStatement.setString(2, visitorsFact.getUrl());
            preparedStatement.setInt(3, visitorsFact.getVisitors());
            preparedStatement.setInt(4, visitorsFact.getVisitors());
            preparedStatement.execute();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void releaseConnection() {
        if( conn != null ) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Realise connection");
    }

    @Override
    protected void finalize() throws Throwable {
        releaseConnection();
    }
}