/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package buildwebgraph;
import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import java.io.*;
/**
 *
 * @author pham0071
 */
public class BuildWebGraph {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
        
        final ImmutableGraph graph = ImmutableGraph.loadSequential("C:\\Users\\pham0071\\Downloads\\uk\\enwiki-2013-t");
        final int n = graph.numNodes();
        System.out.println("Number of nodes: " + n);
        
        File file = new File("C:\\Users\\pham0071\\Downloads\\enwiki.txt");
        Writer output = new BufferedWriter(new FileWriter(file));
        
        int curr = 0;
        //LazyIntIterator successors;
        
        NodeIterator it = graph.nodeIterator();
                
        for (int i = 0; ; i++) {
            
            if (i % 10000 == 0) {
                System.out.println(i);
            }
            
            if (!it.hasNext()) {
                break;
            }
            
            it.skip(1);
                        
            LazyIntIterator successors = it.successors();
            int t;
            while( ( t = successors.nextInt() ) != -1 ) {
                //System.out.print(t + " ");
                output.write(i + " " + t + "\n");
            }
            //System.out.println();
        }
        
        output.close();
    }
}
