package acekg.jgimporter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;

public class JanusClient {
    private String propFileName;

    private Graph graph;
    private GraphTraversalSource g;

    public JanusClient(String propFileName_) {
        propFileName = propFileName_;
        graph = null;
        g = null;
    }

    public void openGraph() {
        graph = JanusGraphFactory.open(propFileName);
        g = graph.traversal();
    }

    public void closeGraph() throws Exception {
        try {
            if (g != null) {
                g.close();
            }
            if (graph != null) {
                graph.close();
            }
        } finally {
            g = null;
            graph = null;
        }
    }

    public JanusGraph getGraph() { return (JanusGraph) graph; }
    public GraphTraversalSource G() { return g; }
    public void GCommit() { g.tx().commit(); }
    public void GRollBack() { g.tx().rollback(); }
}
