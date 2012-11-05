/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package basestationsimulation;

import hla.rti.RTIexception;

/**
 *
 * @author pham0071
 */
public class BaseStationSimulation {

    public static boolean new_event = false;
    public static boolean check = false;
    public static double start_timestamp = 0;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        // TODO code application logic here
        int index = Integer.parseInt(args[0]);
        start_timestamp = Double.parseDouble(args[1]) + 1;
        
        
        try {
            // run the example federate
            
            String federateName = "BaseStationFed" + index;
            new BaseStationFederate(index).runFederate(federateName);
        } catch (RTIexception rtie) {
            // an exception occurred, just log the information and exit
            rtie.printStackTrace();
        }
    }
}
