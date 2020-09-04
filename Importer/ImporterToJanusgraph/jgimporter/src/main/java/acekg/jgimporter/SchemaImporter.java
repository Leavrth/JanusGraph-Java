package acekg.jgimporter;

import com.sun.org.apache.xpath.internal.operations.Bool;
import javafx.util.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Vector;


public class SchemaImporter {
    private static String schemaFileName;
    private static JanusClient jc = null;
    private static boolean isInit = false;
    private static Vector<Pair<String, Boolean>> Props;
    private static HashMap<String, Vector<String>> Vertexes;
    private static HashMap<String, Pair<String, Vector<String>>> Edges;
    public static void setSchemaFileName(final String schemaFileName) { SchemaImporter.schemaFileName = schemaFileName; }
    public static void init(final String propFileName, final String schemaFileName) {
        if (isInit) return;
        SchemaImporter.setSchemaFileName(schemaFileName);
        jc = new JanusClient(propFileName);
        SchemaImporter.createSchema();
        Props = new Vector<>();
        Vertexes = new HashMap<>();
        Edges = new HashMap<>();
        SchemaImporter.isInit = true;
    }
    private static void createSchema() {
        jc.openGraph();
        try (BufferedReader fileIn = new BufferedReader(new FileReader(new File(schemaFileName)))) {
            String line = null;
            while ((line = fileIn.readLine()) != null) {
                switch (line) {
                    case "<P>":
                        while((line = fileIn.readLine()) != null) {
                            if (line.equals("</P>"))
                                break;
                            String [] elements = line.split(",");
                            if (elements.length != 2) continue;
                            Props.add(new Pair<String, Boolean>(elements[0], elements[1].equals("S")?Boolean.TRUE:Boolean.FALSE));
                        }
                        break;
                    case "<V>":
                        while((line = fileIn.readLine()) != null) {
                            if (line.equals("</V>"))
                                break;
                            String [] elements = line.split(",");
                            if (elements.length < 1) continue;
                            Vector<String> con = new Vector<String>();
                            for (int i = 1; i < elements.length; ++i) {
                                con.add(elements[i]);
                            }
                            Vertexes.put(elements[0], con);
                        }
                        break;
                    case "<E>":
                        while((line = fileIn.readLine()) != null) {
                            if (line.equals("</E>"))
                                break;
                            String [] elements = line.split(",");
                            if (elements.length < 2) continue;
                            Vector<String> con = new Vector<String>();
                            for (int i = 2; i < elements.length; ++i) {
                                con.add(elements[i]);
                            }
                            Edges.put(elements[0], new Pair<>(elements[1], con));
                        }
                        break;
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        final JanusGraphManagement management = jc.getGraph().openManagement();
        try {
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            management.commit();
        } catch (Exception e) {
            management.rollback();
            e.printStackTrace();
        } finally {
            try {
                jc.closeGraph();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    private static void createProperties(JanusGraphManagement management) {
        // management.makePropertyKey("reason").dataType(String.class).make();
        // management.makePropertyKey("age").dataType(Integer.class).make();
        for (Pair<String, Boolean> p : Props) {
            if (p.getValue().booleanValue()) { // String
                management.makePropertyKey(p.getKey()).dataType(String.class).make();
            } else {
                management.makePropertyKey(p.getKey()).dataType(Integer.class).make();
            }
        }
    }
    private static void createVertexLabels(JanusGraphManagement management) {
        // management.makeVertexLabel("xxx").make();
        for (Entry<String, Vector<String>> entry : Vertexes.entrySet()) {
            VertexLabel vl = management.makeVertexLabel(entry.getKey()).make();
            for (String s : entry.getValue()) {
                PropertyKey prop = management.getPropertyKey(s);
                management.addProperties(vl, prop);
            }
        }
    }
    private static void createEdgeLabels(JanusGraphManagement management) {
        // management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        // management.makeEdgeLabel("lives").signature(management.getPropertyKey("reason")).make();
        // management.makeEdgeLabel("pet").make();
        for (Entry<String, Pair<String, Vector<String>>> entry : Edges.entrySet()) {
            EdgeLabelMaker elm = management.makeEdgeLabel(entry.getKey());
            switch (entry.getValue().getKey()) {
                case "MANY2ONE":
                    elm.multiplicity(Multiplicity.MANY2ONE).make();
                    break;
            }
        }
    }
    private static void createCompositeIndexes(JanusGraphManagement management) {
        // management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
        for (Pair<String, Boolean> p : Props) {
            management.buildIndex(p.getKey() + "Index", Vertex.class).addKey(management.getPropertyKey(p.getKey())).buildCompositeIndex();
        }
    }
}
