package acekg.janusgraph.importer;

import redis.clients.jedis.Jedis;
import scala.reflect.ClassTag;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.reflect.ClassTag$;

import java.util.Vector;
import java.util.HashMap;
import java.util.Map;

public class MapReduce {
    public static void vertexImporter(final String Label, final String propFileName) {
        long currentTime = System.currentTimeMillis() + (long)(Math.random() * 100000);

        SparkSession ss = SparkSession.builder().appName("VertexImporter" + currentTime)
                .config("spark.io.compression.codec", "snappy")
                .config("spark.default.parallelism", 20)
                .enableHiveSupport().getOrCreate();
        //String Label = args[0];

        JavaRDD<String> lines = ss.sparkContext().textFile("hdfs://10.10.11.5:9000/spark/" + Label + ".txt", 20).toJavaRDD();
        JavaRDD<VertexIDPairs> dataLists = lines.mapPartitions( it -> {
            Vector<VertexIDPairs> vidPs = new Vector<VertexIDPairs>();
            JanusClient jc = new JanusClient(propFileName);
            try {
                jc.openGraph();
                int cnt = 0;
                while (it.hasNext()) {
                    String line = it.next();
                    if (line.equals("")) continue;

                    int curIndex = line.indexOf(' ') + 1;
                    int subToIndex = 0;
                    subToIndex = line.indexOf(' ', curIndex);
                    String vtxLabel = line.substring(curIndex, subToIndex);
                    curIndex = subToIndex + 2;
                    subToIndex = line.indexOf(']', curIndex);
                    long vtxID = Long.parseLong(line.substring(curIndex, subToIndex));
                    curIndex = subToIndex;

                    HashMap<String, String> props = extractProperties(line, curIndex);

                    Vertex v = jc.G().addV(vtxLabel).next();
                    long vid = (long)v.id();

                    v.property("vtxID", vtxID);
                    v.property("vtxLabel", vtxLabel);
                    for (Map.Entry<String, String> entry : props.entrySet()) {
                        v.property(entry.getKey(), entry.getValue());
                    }

                    vidPs.add(new VertexIDPairs(vtxID, vid));
                    ++cnt;
                    if (cnt == 1000) {
                        jc.GCommit();
                        cnt = 0;
                    }
                }
                jc.GCommit();
                jc.closeGraph();
                jc = null;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("ggl");
            }
            return vidPs.iterator();
        } );

        Encoder<VertexIDPairs> vidPsEncoder = Encoders.bean(VertexIDPairs.class);
        Dataset<VertexIDPairs> ds = ss.createDataset(dataLists.rdd(), vidPsEncoder);

        ss.sql("use vertex_id_pairs");

        String viewName = "tmp" + currentTime;
        ds.createOrReplaceTempView(viewName);
        ss.sql("insert into table" + Label + " select * from " + viewName);

        ss.stop();
    }

    public static void HiveToRedis(final String tableName, final String host, final int port) {
        long currentTime = System.currentTimeMillis() + (long) (Math.random() * 100000);

        SparkSession ss = SparkSession.builder().appName("HiveToRedis" + currentTime)
                .config("spark.io.compression.codec", "snappy")
                .config("spark.default.parallelism", 20)
                .enableHiveSupport().getOrCreate();

        ClassTag tag = ClassTag$.MODULE$.apply(String.class);
        Broadcast broadcastTableName = ss.sparkContext().broadcast(tableName, tag);

        ss.sql("use vertex_id_pairs");
        Dataset<Row> ds = ss.sql("select * from " + tableName);
        ds.foreachPartition( it -> {
            Jedis jedis = new Jedis(host, port);
            while (it.hasNext()) {
                // Maybe we can directly use 'TableName' instead of 'broadcastTableName'.
                String locTableName = (String) broadcastTableName.value();
                Row r = it.next();
                String vid = Long.toString((long)r.get(0));
                String vtxid = Long.toString((long)r.get(0));

                jedis.set(locTableName + ":" + vtxid, vid);
            }
            jedis.close();
        } );
    }

    public static void edgeImporter(final String Label, final String propFileName, final String host, final int port) {
        long currentTime = System.currentTimeMillis() + (long)(Math.random() * 100000);

        SparkSession ss = SparkSession.builder()
                .config("spark.io.compression.codec", "snappy")
                .config("spark.default.parallelism", 20)
                .getOrCreate();

        String path = "hdfs://10.10.11.5:9000/spark/" + Label + ".txt";

        JavaRDD<String> lines = ss.sparkContext().textFile(path, 20).toJavaRDD();
        lines.foreachPartition( it-> {
            JanusClient jc = new JanusClient(propFileName);

            try {
                Jedis jedis = new Jedis(host, port);
                jc.openGraph();
                int cnt = 0;
                while (it.hasNext()) {
                    String line = it.next();
                    if (line.length() <= 1) continue;
                    int index = line.indexOf(' ') + 1;
                    int endIndex = line.indexOf(' ', index);
                    String edgeLabel = line.substring(index, endIndex);

                    index = line.indexOf('[', endIndex) + 1;
                    endIndex = line.indexOf(']', index);
                    String vtxLabel1 = line.substring(index, endIndex);

                    index = line.indexOf('[', endIndex) + 1;
                    endIndex = line.indexOf(']', index);
                    String vtxID1 = line.substring(index, endIndex);

                    index = line.indexOf('[', endIndex) + 1;
                    endIndex = line.indexOf(']', index);
                    String vtxLabel2 = line.substring(index, endIndex);

                    index = line.indexOf('[', endIndex) + 1;
                    endIndex = line.indexOf(']', index);
                    String vtxID2 = line.substring(index, endIndex);

                    String vidStr = jedis.get(vtxLabel1 + ":" + vtxID1);
                    if (vidStr == null) continue;
                    long vid1 = Long.parseLong(vidStr);
                    vidStr = jedis.get(vtxLabel2 + ":" + vtxID2);
                    if (vidStr == null) continue;
                    long vid2 = Long.parseLong(vidStr);

                    HashMap<String, String> props = extractProperties(line, endIndex);

                    Edge e = jc.G().V(vid1).as("a").V(vid2).addE(edgeLabel).from("a").next();
                    for (Map.Entry<String, String> entry : props.entrySet()) {
                        e.property(entry.getKey(), entry.getValue());
                    }

                    ++cnt;
                    if (cnt == 1000) {
                        jc.GCommit();
                        cnt = 0;
                    }
                }
                jedis.close();
                jc.GCommit();
                jc.closeGraph();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("ggl");
            }
        } );

    }


    private static HashMap<String, String> extractProperties(final String str, int beginIndex) {
        HashMap<String, String> ret = new HashMap<String, String>();
        int curIndex = beginIndex + 1; // '['
        int endToIndex = 0;
        StringBuilder sbKey = null;
        StringBuilder sbValue = null;
        final int strLength = str.length();
        for (; curIndex < strLength; ++curIndex) {
            if (str.charAt(curIndex) == '[') {
                ++curIndex;
                endToIndex = str.indexOf(',', curIndex);
                String keyStr = str.substring(curIndex, endToIndex);
                curIndex = endToIndex + 1;
                String valueStr = null;
                if (str.charAt(curIndex) == 's') { // string
                    curIndex += 8;
                    endToIndex = str.indexOf("\"]", curIndex);
                    valueStr = str.substring(curIndex, endToIndex);
                } else { // long
                    curIndex += 6;
                    endToIndex = str.indexOf("\"]", curIndex);
                    valueStr = str.substring(curIndex, endToIndex);
                }
                curIndex = endToIndex + 1;
                ret.put(keyStr, valueStr);
            }
        }
        return ret;
    }
}
