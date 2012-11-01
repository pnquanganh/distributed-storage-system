/*
 *   Copyright 2007 The Portico Project
 *
 *   This file is part of portico.
 *
 *   portico is free software; you can redistribute it and/or modify
 *   it under the terms of the Common Developer and Distribution License (CDDL) 
 *   as published by Sun Microsystems. For more information see the LICENSE file.
 *   
 *   Use of this software is strictly AT YOUR OWN RISK!!!
 *   If something bad happens you do not have permission to come crying to me.
 *   (that goes for your lawyer as well)
 *
 */
package simulationassignment;

import Event.CallInitEvent;
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
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

import org.portico.impl.hla13.types.DoubleTime;
import org.portico.impl.hla13.types.DoubleTimeInterval;

/**
 * This is an example federate demonstrating how to properly use the HLA 1.3
 * Java interface supplied with Portico.
 *
 * As it is intended for example purposes, this is a rather simple federate. The
 * process is goes through is as follows:
 *
 * 1. Create the RTIambassador 2. Try to create the federation (nofail) 3. Join
 * the federation 4. Announce a Synchronization Point (nofail) 5. Wait for the
 * federation to Synchronized on the point 6. Enable Time Regulation and
 * Constrained 7. Publish and Subscribe 8. Register an Object Instance 9. Main
 * Simulation Loop (executes 20 times) 9.1 Update attributes of registered
 * object 9.2 Send an Interaction 9.3 Advance time by 1.0 10. Delete the Object
 * Instance 11. Resign from Federation 12. Try to destroy the federation
 * (nofail)
 *
 * NOTE: Those items marked with (nofail) deal with situations where multiple
 * federates may be working in the federation. In this sitaution, the federate
 * will attempt to carry out the tasks defined, but it won't stop or exit if
 * they fail. For example, if another federate has already created the
 * federation, the call to create it again will result in an exception. The
 * example federate expects this and will not fail. NOTE: Between actions 4. and
 * 5., the federate will pause until the uses presses the enter key. This will
 * give other federates a chance to enter the federation and prevent other
 * federates from racing ahead.
 *
 *
 * The main method to take notice of is {@link #runFederate(String)}. It
 * controls the main simulation loop and triggers most of the important
 * behaviour. To make the code simpler to read and navigate, many of the
 * important HLA activities are broken down into separate methods. For example,
 * if you want to know how to send an interaction, see the
 * {@link #sendInteraction()} method.
 *
 * With regard to the FederateAmbassador, it will log all incoming information.
 * Thus, if it receives any reflects or interactions etc... you will be notified
 * of them.
 *
 * Note that all of the methods throw an RTIexception. This class is the parent
 * of all HLA exceptions. The HLA Java interface is full of exceptions, with
 * only a handful being actually useful. To make matters worse, they're all
 * checked exceptions, so unlike C++, we are forced to handle them by the
 * compiler. This is unnecessary in this small example, so we'll just throw all
 * exceptions out to the main method and handle them there, rather than handling
 * each exception independently as they arise.
 */
public class SourceFederate {
    //----------------------------------------------------------
    //                    STATIC VARIABLES
    //----------------------------------------------------------

    public static int flag = 0;
    public static int flag1 = 0;
    /**
     * The number of times we will update our attributes and send an interaction
     */
    public static final int ITERATIONS = 7;
    /**
     * The sync point all federates will sync up on before starting
     */
    public static final String READY_TO_RUN = "ReadyToRun";
    public static final int NUM_FED = 5;
    public static final int NUM_BASE = 4;
    //----------------------------------------------------------
    //                   INSTANCE VARIABLES
    //----------------------------------------------------------
    private RTIambassador rtiamb;
    private SourceFederateAmbassador fedamb;
    public PriorityQueue<CallInitEvent> event_list = new PriorityQueue<>();

    //----------------------------------------------------------
    //                      CONSTRUCTORS
    //----------------------------------------------------------
    //----------------------------------------------------------
    //                    INSTANCE METHODS
    //----------------------------------------------------------
    /**
     * This is just a helper method to make sure all logging it output in the
     * same form
     */
    private void log(String message) {
        System.out.println("ExampleFederate   : " + message);
    }

    /**
     * This method will block until the user presses enter
     */
    private void waitForUser() {
        log(" >>>>>>>>>> Press Enter to Continue <<<<<<<<<<");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
        } catch (Exception e) {
            log("Error while waiting for user input: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * As all time-related code is Portico-specific, we have isolated it into a
     * single method. This way, if you need to move to a different RTI, you only
     * need to change this code, rather than more code throughout the whole
     * class.
     */
    private LogicalTime convertTime(double time) {
        // PORTICO SPECIFIC!!
        return new DoubleTime(time);
    }

    /**
     * Same as for {@link #convertTime(double)}
     */
    private LogicalTimeInterval convertInterval(double time) {
        // PORTICO SPECIFIC!!
        return new DoubleTimeInterval(time);
    }

    ///////////////////////////////////////////////////////////////////////////
    ////////////////////////// Main Simulation Method /////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    /**
     * This is the main simulation loop. It can be thought of as the main method
     * of the federate. For a description of the basic flow of this federate,
     * see the class level comments
     */
    public void runFederate(String federateName) throws RTIexception {
        /////////////////////////////////
        // 1. create the RTIambassador //
        /////////////////////////////////
        rtiamb = RtiFactoryFactory.getRtiFactory().createRtiAmbassador();

        //////////////////////////////
        // 2. create the federation //
        //////////////////////////////
        // create
        // NOTE: some other federate may have already created the federation,
        //       in that case, we'll just try and join it
        try {
            File fom = new File("testfom.fed");
            rtiamb.createFederationExecution("ExampleFederation",
                    fom.toURI().toURL());
            log("Created Federation");

        } catch (FederationExecutionAlreadyExists exists) {
            log("Didn't create federation, it already existed");
        } catch (MalformedURLException urle) {
            log("Exception processing fom: " + urle.getMessage());
            urle.printStackTrace();
            return;
        }

        ////////////////////////////
        // 3. join the federation //
        ////////////////////////////
        // create the federate ambassador and join the federation
        fedamb = new SourceFederateAmbassador();
        rtiamb.joinFederationExecution(federateName, "ExampleFederation", fedamb);
        log("Joined Federation as " + federateName);

        rtiamb.registerFederationSynchronizationPoint(READY_TO_RUN, null);

        // wait until the point is announced
        while (fedamb.isAnnounced == false) {
            rtiamb.tick();
        }

        // create region
        int rshld = rtiamb.getRoutingSpaceHandle("TestSpace");
        int dimhld = rtiamb.getDimensionHandle("TestDimension", rshld);

        Region[] sRegion = new Region[NUM_FED];
        for (int i = 0; i < sRegion.length; i++) {
            sRegion[i] = rtiamb.createRegion(rshld, 1);
            sRegion[i].setRangeLowerBound(0, dimhld, i * 2);
            sRegion[i].setRangeUpperBound(0, dimhld, i * 2 + 1);
            rtiamb.notifyOfRegionModification(sRegion[i]);
        }

        publishAndSubscribe();
        log("Published and Subscribed");
        enableTimePolicy();

        waitForUser();

        int count = 0;
        for (CallInitEvent entry : event_list) {
            Double event_time = entry.time_event;
            CallInitEvent event = entry;

            System.out.println(event_time.toString() + ": " + event.toString());

            sendInteraction(sRegion[event.station / NUM_BASE], event_time, event.speed, event.station,
                    event.position, event.duration);
            log("Time Advanced to " + fedamb.federateTime);

        }

        rtiamb.synchronizationPointAchieved(READY_TO_RUN);
        log("Achieved sync point: " + READY_TO_RUN + ", waiting for federation...");
        while (fedamb.isReadyToRun == false) {
            rtiamb.tick();
        }

        waitForUser();
        advanceTime(20000);
        log("Time Advanced to " + fedamb.federateTime);
        log("done");
        waitForUser();

        ////////////////////////////////////
        // 11. resign from the federation //
        ////////////////////////////////////
        rtiamb.resignFederationExecution(ResignAction.NO_ACTION);
        log("Resigned from Federation");

        ////////////////////////////////////////
        // 12. try and destroy the federation //
        ////////////////////////////////////////
        // NOTE: we won't die if we can't do this because other federates
        //       remain. in that case we'll leave it for them to clean up
        try {
            rtiamb.destroyFederationExecution("ExampleFederation");
            log("Destroyed Federation");
        } catch (FederationExecutionDoesNotExist dne) {
            log("No need to destroy federation, it doesn't exist");
        } catch (FederatesCurrentlyJoined fcj) {
            log("Didn't destroy federation, federates still joined");
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Helper Methods //////////////////////////////
    ////////////////////////////////////////////////////////////////////////////
    /**
     * This method will attempt to enable the various time related properties
     * for the federate
     */
    private void enableTimePolicy() throws RTIexception {
        // NOTE: Unfortunately, the LogicalTime/LogicalTimeInterval create code is
        //       Portico specific. You will have to alter this if you move to a
        //       different RTI implementation. As such, we've isolated it into a
        //       method so that any change only needs to happen in a couple of spots 
        LogicalTime currentTime = convertTime(fedamb.federateTime);
        LogicalTimeInterval lookahead = convertInterval(fedamb.federateLookahead);

        ////////////////////////////
        // enable time regulation //
        ////////////////////////////
        this.rtiamb.enableTimeRegulation(currentTime, lookahead);

        // tick until we get the callback
        while (fedamb.isRegulating == false) {
            rtiamb.tick();
        }

        /////////////////////////////
        // enable time constrained //
        /////////////////////////////
        this.rtiamb.enableTimeConstrained();

        // tick until we get the callback
        while (fedamb.isConstrained == false) {
            rtiamb.tick();
        }
    }

    /**
     * This method will inform the RTI about the types of data that the federate
     * will be creating, and the types of data we are interested in hearing
     * about as other federates produce it.
     */
    //private void publishAndSubscribe(Region region) throws RTIexception
    private void publishAndSubscribe() throws RTIexception {


        int classHandle = rtiamb.getInteractionClassHandle("InteractionRoot.Call_init");

        rtiamb.publishInteractionClass(classHandle);
    }

    /**
     * This method will register an instance of the class ObjectRoot.A and will
     * return the federation-wide unique handle for that instance. Later in the
     * simulation, we will update the attribute values for this instance
     */
    //private int registerObject(Region region) throws RTIexception
    private int registerObject(Region region) throws RTIexception {
        int classHandle = rtiamb.getObjectClassHandle("ObjectRoot.A");
        return rtiamb.registerObjectInstance(classHandle);

    }

    /**
     * This method will update all the values of the given object instance. It
     * will set each of the values to be a string which is equal to the name of
     * the attribute plus the current time. eg "aa:10.0" if the time is 10.0.
     * <p/>
     * Note that we don't actually have to update all the attributes at once, we
     * could update them individually, in groups or not at all!
     */
    private void updateAttributeValues(int objectHandle, int loop) throws RTIexception {
        ///////////////////////////////////////////////
        // create the necessary container and values //
        ///////////////////////////////////////////////
        // create the collection to store the values in, as you can see
        // this is quite a lot of work
        SuppliedAttributes attributes =
                RtiFactoryFactory.getRtiFactory().createSuppliedAttributes();

        // generate the new values
        // we use EncodingHelpers to make things nice friendly for both Java and C++
        byte[] aaValue = EncodingHelpers.encodeString("aa:" + Integer.toString(loop));//getLbts() );
        byte[] abValue = EncodingHelpers.encodeString("ab:" + Integer.toString(loop));//getLbts() );
        byte[] acValue = EncodingHelpers.encodeString("ac:" + Integer.toString(loop));//getLbts() );

        // get the handles
        // this line gets the object class of the instance identified by the
        // object instance the handle points to
        int classHandle = rtiamb.getObjectClass(objectHandle);
        int aaHandle = rtiamb.getAttributeHandle("aa", classHandle);
        int abHandle = rtiamb.getAttributeHandle("ab", classHandle);
        int acHandle = rtiamb.getAttributeHandle("ac", classHandle);

        // put the values into the collection
        attributes.add(aaHandle, aaValue);
//		attributes.add( abHandle, abValue );
//		attributes.add( acHandle, acValue );

        //////////////////////////
        // do the actual update //
        //////////////////////////
//        rtiamb.updateAttributeValues(objectHandle, attributes, generateTag());

        // note that if you want to associate a particular timestamp with the
        // update. here we send another update, this time with a timestamp:
        LogicalTime time = convertTime(fedamb.federateTime + fedamb.federateLookahead);
        rtiamb.updateAttributeValues(objectHandle, attributes, generateTag(), time);
    }

    /**
     * This method will send out an interaction of the type InteractionRoot.X.
     * Any federates which are subscribed to it will receive a notification the
     * next time they tick(). Here we are passing only two of the three
     * parameters we could be passing, but we don't actually have to pass any at
     * all!
     */
    private void sendInteraction(Region region, int loop) throws RTIexception {
        ///////////////////////////////////////////////
        // create the necessary container and values //
        ///////////////////////////////////////////////
        // create the collection to store the values in
        SuppliedParameters parameters =
                RtiFactoryFactory.getRtiFactory().createSuppliedParameters();

        // generate the new values
        // we use EncodingHelpers to make things nice friendly for both Java and C++
        byte[] xaValue = EncodingHelpers.encodeString("xa:" + Integer.toString(loop));// getLbts());
        byte[] xbValue = EncodingHelpers.encodeString("xb:" + Integer.toString(loop));//getLbts());

        // get the handles
        int classHandle = rtiamb.getInteractionClassHandle("InteractionRoot.X");
        int xaHandle = rtiamb.getParameterHandle("xa", classHandle);
        int xbHandle = rtiamb.getParameterHandle("xb", classHandle);

        // put the values into the collection
        parameters.add(xaHandle, xaValue);
        parameters.add(xbHandle, xbValue);

        LogicalTime time = convertTime(fedamb.federateTime + ((flag == 0) ? 1.0 : 2.0));
        //rtiamb.sendInteraction(classHandle, parameters, generateTag(), time);
        //rtiamb.sendInteractionWithRegion(classHandle, parameters, generateTag(), region, time);
        rtiamb.sendInteractionWithRegion(classHandle, parameters, generateTag(), region);
    }

    private void sendInteraction(Region region,
            double time_event, double speed, int station,
            double position, double duration) throws RTIexception {
        ///////////////////////////////////////////////
        // create the necessary container and values //
        ///////////////////////////////////////////////
        // create the collection to store the values in
        SuppliedParameters parameters =
                RtiFactoryFactory.getRtiFactory().createSuppliedParameters();

        // generate the new values
        // we use EncodingHelpers to make things nice friendly for both Java and C++
        byte[] timeValue = EncodingHelpers.encodeString(Double.toString(time_event));// getLbts());
        byte[] speedValue = EncodingHelpers.encodeString(Double.toString(speed));// getLbts());
        byte[] stationValue = EncodingHelpers.encodeString(Integer.toString(station));//getLbts());
        byte[] positionValue = EncodingHelpers.encodeString(Double.toString(position));//getLbts());
        byte[] durationValue = EncodingHelpers.encodeString(Double.toString(duration));//getLbts());

        // get the handles
        int classHandle = rtiamb.getInteractionClassHandle("InteractionRoot.Call_init");
        int timeHandle = rtiamb.getParameterHandle("time", classHandle);
        int speedHandle = rtiamb.getParameterHandle("speed", classHandle);
        int stationHandle = rtiamb.getParameterHandle("station", classHandle);
        int positionHandle = rtiamb.getParameterHandle("position", classHandle);
        int durationHandle = rtiamb.getParameterHandle("duration", classHandle);


        // put the values into the collection
        parameters.add(timeHandle, timeValue);
        parameters.add(speedHandle, speedValue);
        parameters.add(stationHandle, stationValue);
        parameters.add(positionHandle, positionValue);
        parameters.add(durationHandle, durationValue);

        rtiamb.notifyOfRegionModification(region);

        LogicalTime time = convertTime(time_event); // + getLbts());
//        rtiamb.sendInteraction(classHandle, parameters, generateTag(), time);
        rtiamb.sendInteractionWithRegion(classHandle, parameters, generateTag(), region, time);
//        rtiamb.sendInteractionWithRegion(classHandle, parameters, generateTag(), region);
    }

    /**
     * This method will request a time advance to the current time, plus the
     * given timestep. It will then wait until a notification of the time
     * advance grant has been received.
     */
    private void advanceTime(double timestep) throws RTIexception {
        // request the advance
        fedamb.isAdvancing = true;
//        LogicalTime newTime = convertTime(fedamb.federateTime + timestep);
        LogicalTime newTime = convertTime(timestep);

        rtiamb.nextEventRequest(newTime);

        // wait for the time advance to be granted. ticking will tell the
        // LRC to start delivering callbacks to the federate
        while (fedamb.isAdvancing) {
            rtiamb.tick();
        }
    }

    /**
     * This method will attempt to delete the object instance of the given
     * handle. We can only delete objects we created, or for which we own the
     * privilegeToDelete attribute.
     */
    private void deleteObject(int handle) throws RTIexception {
        rtiamb.deleteObjectInstance(handle, generateTag());
    }

    private double getLbts() {
        return fedamb.federateTime + fedamb.federateLookahead;
    }

    private byte[] generateTag() {
        return ("" + System.currentTimeMillis()).getBytes();
    }
    
}