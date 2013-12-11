/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package buildwebgraph;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import it.unimi.dsi.webgraph.NodeIterator;

/**
 *
 * @author pham0071
 */
public class BuildWebGraph {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
        
        final ImmutableGraph graph = ImmutableGraph.loadSequential("C:\\Users\\pham0071\\Downloads\\uk\\uk-2007-05@1000000-t");
        final int n = graph.numNodes();
        System.out.println("Number of nodes: " + n);
        
        int curr = 0;
        //LazyIntIterator successors;
        
        NodeIterator it = graph.nodeIterator();
        
        int MinOutDegree = Integer.MAX_VALUE;
        int MaxOutDegree = -1;
        for (int i = 0; i < 20; i++) {
            
//            if (i % 10000 == 0)
//                System.out.println(i);
            it.skip(1);
            int d = it.outdegree();
//            if (d < MinOutDegree) {
//                MinOutDegree = d;
//            }
//            
//            if (d > MaxOutDegree){
//                MaxOutDegree = d;
//            }
            
            System.out.println("Outdegree: " + d);
            LazyIntIterator successors = it.successors();
            int t;
            while( ( t = successors.nextInt() ) != -1 ) {
                System.out.print(t + " ");
            }
            System.out.println();
        }
        
//        System.out.println("Min out degree: " + MinOutDegree);
//        System.out.println("Max out degree: " + MaxOutDegree);
        
        
//        int[] succ = it.successorArray();
//        for (int i : succ) {
//            System.out.print(i + " ");
//        }
        
    }
}
