package com.bread.project.gremlin_example;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App 
{
    public static void main( String[] args )
    {
    	
    	try {
	    	Graph graph = EmptyGraph.instance();
	    	GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
	    	
	    	for(int i = 0; i != 1000; ++i) {
		        try {
		            
		            Object herculesAge = g.V().has("name", "stu" + i).values("age").next();
		            System.out.println( "stu" + i + " is " + herculesAge + " years old.");
		            
		        } catch(Exception e) {
		            e.printStackTrace();
		        }
	    	}
	    	g.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
