/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package simulationassignment;

import Event.CallInitEvent;
import Event.EventTypes;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

import hla.rti.AttributeHandleSet;
import hla.rti.FederatesCurrentlyJoined;
import hla.rti.FederationExecutionAlreadyExists;
import hla.rti.FederationExecutionDoesNotExist;
import hla.rti.LogicalTime;
import hla.rti.LogicalTimeInterval;
import hla.rti.RTIambassador;
import hla.rti.RTIexception;
import hla.rti.Region;
import hla.rti.ResignAction;
import hla.rti.SuppliedAttributes;
import hla.rti.SuppliedParameters;
import hla.rti.jlc.EncodingHelpers;
import hla.rti.jlc.RtiFactoryFactory;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.portico.impl.hla13.types.DoubleTime;
import org.portico.impl.hla13.types.DoubleTimeInterval;

/**
 *
 * @author pham0071
 */
public class SourceSimulation {

    
    public static void read_data(PriorityQueue<CallInitEvent> event_list){
        BufferedReader br = null;
        Random r = new Random();
        try {
            br = new BufferedReader(new FileReader("data.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                System.out.println(line);
                Scanner scanner = new Scanner(line);
                int num = scanner.nextInt();
                double event_time = scanner.nextDouble() + 1.0;
                int station = scanner.nextInt() - 1;
                double duration = scanner.nextDouble();
                double speed = scanner.nextDouble() * 1000 / 3600;
                
                double position = r.nextDouble() * 2000.0;
                event_list.add(new CallInitEvent(EventTypes.CALL_INITIATION, event_time, speed, station, position, duration));
            }
            br.close();
        }  catch (FileNotFoundException ex) {
            Logger.getLogger(SourceSimulation.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SourceSimulation.class.getName()).log(Level.SEVERE, null, ex);
        }finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(SourceSimulation.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        String federateName = "sourceFederate";
        if (args.length != 0) {
            federateName = args[0];
        }

        try {
            // run the example federate
            SourceFederate sf = new SourceFederate();
            read_data(sf.event_list);
            sf.runFederate(federateName);
        } catch (RTIexception rtie) {
            // an exception occurred, just log the information and exit
            rtie.printStackTrace();
        }
    }
}
