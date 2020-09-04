package acekg.jgimporter;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Importer {
    private static String driver = "com.mysql.cj.jdbc.Driver";
    private static String user = "root";
    private static String password = "123456";
    private static String url = "jdbc:mysql://10.10.10.10:3306/janusgraph?" +
            "useUnicode=true&characterEncoding=UTF8&useSSL=true&serverTimezone=Asia/Shanghai";
    private static String host = null;
    private static int port = 0;
    public static void setUser(final String user_) { Importer.user = user_; }
    public static void setPassword(final String password_) { Importer.password = password_; }
    public static void setUrl(final String host, final String port, final String database) {
        Importer.url = "jdbc:mysql://"+ host +":"+ port + "/" + database + "?" +
                "useUnicode=true&characterEncoding=UTF8&useSSL=true&serverTimezone=Asia/Shanghai";
    }
    public static void setHost(final String host) { Importer.host = host; }
    public static void setPort(final int port) { Importer.port = port; }
    public static void vertexImporter(final String propFileName, final String dataFileName, final String Label) {
        Connection conn;
        JanusClient jc = new JanusClient(propFileName);
        try (BufferedReader fileIn = new BufferedReader(new FileReader(new File(dataFileName)))) {
            // open mysql connection
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database.");

            // batch insert setting
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement("insert ignore into " + Label + " (vtxid, vid) values (?, ?)");
            pstmt.clearBatch();
            // open JanusGraph
            jc.openGraph();

            String line = null;
            int cnt = 0;

            while ((line = fileIn.readLine()) != null) {
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

                pstmt.setLong(1, vtxID);
                pstmt.setLong(2, vid);
                pstmt.addBatch();

                ++cnt;
                if (cnt % 1000 == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                }
                if (cnt == 10000) {
                    jc.GCommit();
                    conn.commit();
                    cnt = 0;
                }
            }
            jc.GCommit();
            pstmt.executeBatch();
            conn.commit();
            jc.closeGraph();
            jc = null;
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void edgeImporter(final String propFileName, final String dataFileName) {
        JanusClient jc = new JanusClient(propFileName);
        try (BufferedReader fileIn = new BufferedReader(new FileReader(new File(dataFileName)))) {
            Jedis jedis = new Jedis(host, port);
            jc.openGraph();
            String line = null;
            int cnt = 0;

            while ((line = fileIn.readLine()) != null) {
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
                if (cnt == 10000) {
                    jc.GCommit();
                    cnt = 0;
                }
            }
            jedis.close();
            jc.GCommit();
            jc.closeGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
