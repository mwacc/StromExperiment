package storm.experiment2.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by kostya on 4/18/14.
 */
public class SocketReaderSpout extends BaseRichSpout {
    private String uuid = UUID.randomUUID().toString();


    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    private SpoutOutputCollector collector;
    private String hostAddress = "<unknown>";

    public SocketReaderSpout() {
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();

            System.out.println(String.format("\n\n  =>  %s on %s  <=  \n\n", uuid, InetAddress.getLocalHost().getHostName()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("originalFrom","message", "length", "createdAt"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        new Thread(new SocketListener()).start();
    }

    @Override
    public void nextTuple() {
        String msg = queue.poll();
        if (msg != null) {
            collector.emit(new Values(hostAddress, msg, String.valueOf(msg.length()), new Date().toString()));
        } else {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    class SocketListener implements Runnable {
        private int[] sockets = new int[]{40002, 40003, 40004, 40005};

        @Override
        public void run() {
            ServerSocket server = null;
            Socket socket = null;

            try {
                while( true ) {
                    for(int socketport : sockets) {
                        try {
                            System.out.println("\tTry spout " +uuid+" on " + hostAddress +
                                    ":" + socketport);
                            server = new ServerSocket(socketport);
                            //creating socket and waiting for client connection
                            socket = server.accept();

                            break;
                        } catch(IOException io) {
                            continue;
                        }
                    }
                    System.out.println("\n\tSpout "+uuid+" is ready on host " + hostAddress +
                            ":" + socket.getLocalPort() + "\n");


                    //read from socket to ObjectInputStream object
                    InputStream socketInputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socketInputStream));


                    String line;
                    while ((line = in.readLine()) != null) {
                        queue.put(line);
                    }
                    socket.close();
                    server.close();
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            } finally {
                if (server != null) {
                    try {
                        server.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
    }

}
