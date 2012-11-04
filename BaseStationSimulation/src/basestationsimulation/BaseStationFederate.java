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
package basestationsimulation;

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
import java.util.SortedMap;
import java.util.TreeMap;

import org.portico.impl.hla13.types.DoubleTime;
import org.portico.impl.hla13.types.DoubleTimeInterval;

import Event.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class BaseStationFederate {
    //----------------------------------------------------------
    //                    STATIC VARIABLES
    //----------------------------------------------------------

    public static int flag = 0;
    public static int flag1 = 0;
    /**
     * The number of times we will update our attributes and send an interaction
     */
    public static final int ITERATIONS = 7;
    public static final double COVER_RANGE = 2000.0;
    public static final int NUM_BASE = 4;
    public static final int NUM_FED = 5;
    /**
     * The sync point all federates will sync up on before starting
     */
    public static final String READY_TO_RUN = "ReadyToRun";
    //----------------------------------------------------------
    //                   INSTANCE VARIABLES
    //----------------------------------------------------------
    private RTIambassador rtiamb;
    private BaseStationFederateAmbassador fedamb;
    public PriorityQueue<Events> event_list = new PriorityQueue<>();
    private int index;
    public int[] free_channels = {10, 10, 10, 10};
    public double current_time;
    public boolean new_event = false;
    public int num_call_init = 0;
    public int num_call_handover = 0;
    public int num_call_handover_from_other_fed = 0;
    public int num_blocked_call = 0;
    public int num_dropped_call = 0;

    public BaseStationFederate(int index) {
        this.index = index;

    }

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

    private double RoundDouble(double input) {
        return Math.floor(1000 * input + 0.5) / 1000;
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

        ////////////////////////////
        // 3. join the federation //
        ////////////////////////////
        // create the federate ambassador and join the federation
        fedamb = new BaseStationFederateAmbassador();
        fedamb.event_list = event_list;
        fedamb.rtiamb = rtiamb;

        rtiamb.joinFederationExecution(federateName, "ExampleFederation", fedamb);
        log("Joined Federation as " + federateName);

        // create region
        int rshld = rtiamb.getRoutingSpaceHandle("TestSpace");
        int dimhld = rtiamb.getDimensionHandle("TestDimension", rshld);
        Region sRegion = rtiamb.createRegion(rshld, 1);

        sRegion.setRangeLowerBound(0, dimhld, index * 2);
        sRegion.setRangeUpperBound(0, dimhld, index * 2 + 1);
        rtiamb.notifyOfRegionModification(sRegion);

        Region uRegion = rtiamb.createRegion(rshld, 1);
        uRegion.setRangeLowerBound(0, dimhld, (index + 1) * 2);
        uRegion.setRangeUpperBound(0, dimhld, (index + 1) * 2 + 1);
        rtiamb.notifyOfRegionModification(uRegion);

        publishAndSubscribe(sRegion);
        log("Published and Subscribed");

        enableTimePolicy();

        ///////////////////////////////////////////////////////
        // 5. achieve the point and wait for synchronization //
        ///////////////////////////////////////////////////////
        // tell the RTI we are ready to move past the sync point and then wait
        // until the federation has synchronized on
        rtiamb.synchronizationPointAchieved(READY_TO_RUN);
        log("Achieved sync point: " + READY_TO_RUN + ", waiting for federation...");
        while (fedamb.isReadyToRun == false) {
            rtiamb.tick();
        }

        double last_event_time = 20000;
        while (event_list.isEmpty()) {
            advanceTime(last_event_time);
            log("0. Time Advanced to " + fedamb.federateTime);
        }

        while (!event_list.isEmpty() || fedamb.federateTime < last_event_time) {
            try {
                if (event_list.isEmpty() && fedamb.federateTime < last_event_time) {
                    advanceTime(last_event_time);
                    log("1. Time Advanced to " + fedamb.federateTime);
                } else {
                    Events entry = event_list.peek();
                    double event_time = entry.event_time;
                    BaseStationSimulation.new_event = false;
                    log("fed time " + fedamb.federateTime + ", event time " + event_time);
                    if (fedamb.federateTime < event_time) {
                        advanceTime(event_time);
                        log("2. Time Advanced to " + fedamb.federateTime);
                    }
//                    else if (fedamb.federateTime < event_time) {
//                        event_list.poll();
//                        continue;
//                    }

                    if (!BaseStationSimulation.new_event && fedamb.federateTime < event_time)
                        continue;
                    
                    if (BaseStationSimulation.new_event) {
                        entry = event_list.peek();
                        event_time = entry.event_time;
                    }
                    event_list.poll();

                    EventHandling(uRegion, entry, event_time);
                }
            } catch (Exception ex) {
                Logger.getLogger(BaseStationFederate.class.getName()).log(Level.SEVERE, null, ex);
            }

        }


        System.out.println("Call init: " + num_call_init);
        System.out.println("Call handover: " + num_call_handover);
        System.out.println("Call handover from other feds: " + num_call_handover_from_other_fed);
        System.out.println("Call blocked: " + num_blocked_call);
        System.out.println("Call dropped: " + num_dropped_call);
        waitForUser();

        ////////////////////////////////////
        // 11. resign from the federation //
        ////////////////////////////////////
        rtiamb.resignFederationExecution(ResignAction.NO_ACTION);
        log("Resigned from Federation");

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
    private void publishAndSubscribe(Region region) throws RTIexception {

        int classHandle = rtiamb.getInteractionClassHandle("InteractionRoot.Call_init");
        int classHandle1 = rtiamb.getInteractionClassHandle("InteractionRoot.Handover");

        rtiamb.publishInteractionClass(classHandle1); //, attributes);

        rtiamb.subscribeInteractionClassWithRegion(classHandle, region); //, attributes);
        rtiamb.subscribeInteractionClassWithRegion(classHandle1, region); //, attributes);

        int rshld = rtiamb.getRoutingSpaceHandle("TestSpace");
        int dimhld = rtiamb.getDimensionHandle("TestDimension", rshld);
        log("Subscribed with region:" + region.getRangeLowerBound(0, dimhld) + " : "
                + region.getRangeUpperBound(0, dimhld));
    }

    /**
     * This method will register an instance of the class ObjectRoot.A and will
     * return the federation-wide unique handle for that instance. Later in the
     * simulation, we will update the attribute values for this instance
     */
    //private int registerObject(Region region) throws RTIexception
    private int registerObject(Region region) throws RTIexception {
        int[] attributes = new int[1];
        int classHandle = rtiamb.getObjectClassHandle("ObjectRoot.A");
        System.out.println("classHandle = " + classHandle);
        attributes[0] = rtiamb.getAttributeHandle("aa", classHandle);

        Region[] regions = new Region[1];
        regions[0] = region;

        int rshld = rtiamb.getRoutingSpaceHandle("TestSpace");
        int dimhld = rtiamb.getDimensionHandle("TestDimension", rshld);
        log("Registered with region:" + region.getRangeLowerBound(0, dimhld) + " : "
                + region.getRangeUpperBound(0, dimhld));

        return rtiamb.registerObjectInstanceWithRegion(classHandle, attributes, regions);

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
        rtiamb.updateAttributeValues(objectHandle, attributes, generateTag());

        // note that if you want to associate a particular timestamp with the
        // update. here we send another update, this time with a timestamp:
        //LogicalTime time = convertTime( fedamb.federateTime + fedamb.federateLookahead );
        //rtiamb.updateAttributeValues( objectHandle, attributes, generateTag(), time );
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
            double time_event, double speed, double duration) throws RTIexception {
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
        byte[] durationValue = EncodingHelpers.encodeString(Double.toString(duration));//getLbts());

        // get the handles
        int classHandle = rtiamb.getInteractionClassHandle("InteractionRoot.Handover");
        int timeHandle = rtiamb.getParameterHandle("time", classHandle);
        int speedHandle = rtiamb.getParameterHandle("speed", classHandle);
        int durationHandle = rtiamb.getParameterHandle("duration", classHandle);


        // put the values into the collection
        parameters.add(timeHandle, timeValue);
        parameters.add(speedHandle, speedValue);
        parameters.add(durationHandle, durationValue);

        //////////////////////////
        // send the interaction //
        //////////////////////////
        rtiamb.notifyOfRegionModification(region);

        int rshld1 = rtiamb.getRoutingSpaceHandle("TestSpace");
        int dimhld1 = rtiamb.getDimensionHandle("TestDimension", rshld1);

        log("sendInteractionWithRegion:" + region.getRangeLowerBound(0, dimhld1) + " : "
                + region.getRangeUpperBound(0, dimhld1));

        LogicalTime time = convertTime(time_event);// + getLbts());
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
        int count = 0;
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

    private void EventHandling(Region region, Events entry, double event_time) throws AssertionError {

        System.out.println("Free channels: " + free_channels[0] + " "
                + free_channels[1] + " "
                + free_channels[2] + " "
                + free_channels[3]);

        switch (entry.event_type) {
            case CALL_INITIATION:
                HandleCallInitEvent(entry, event_time, region);
                break;
            case CALL_HANDOVER:
                HandleHandoverEvent(entry, event_time, region);
                break;
            case CALL_TERMINATION:
                HandleCallTermEvent(entry);
                break;
            default:
                throw new AssertionError();
        }
    }

    private boolean HandleCallInitEvent(Events entry, double event_time, Region region) {

        num_call_init++;
        CallInitEvent call_init_event = (CallInitEvent) entry;
        System.out.println("HandleCallInitEvent " + call_init_event.toString());

        int station = call_init_event.station;
        if (free_channels[station % NUM_BASE] == 0) {
            num_blocked_call++;
            return true; // blocked call
        }
        free_channels[station % NUM_BASE] -= 1;
        double speed = call_init_event.speed;
        double position = call_init_event.position;
        double duration = call_init_event.duration;
        if (RoundDouble(speed * duration + position) < COVER_RANGE) {
            CallTermEvent call_term_ev = new CallTermEvent(EventTypes.CALL_TERMINATION, RoundDouble(event_time + duration), station);
            event_list.add(call_term_ev);

        } else {
            double elapsed_time = RoundDouble((COVER_RANGE - position) / speed);
            int next_station = station + 1;
            double next_event_time = RoundDouble(event_time + elapsed_time);
            double remaining_duration = RoundDouble(duration - elapsed_time);
            if (next_station >= (index + 1) * NUM_BASE) {
                if (index <= NUM_FED - 2) {
                    try {
                        log("Before send " + fedamb.federateTime);
                        // send interaction
                        sendInteraction(region, next_event_time, speed, remaining_duration);

                    } catch (RTIexception ex) {
                        Logger.getLogger(BaseStationFederate.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
            CallHandoverEvent call_hand_ev = new CallHandoverEvent(EventTypes.CALL_HANDOVER, next_event_time,
                    speed, station, next_station, remaining_duration);
            event_list.add(call_hand_ev);

        }
        return false;
    }

    private boolean HandleHandoverEvent(Events entry, double event_time, Region region) {

        CallHandoverEvent call_hand_event = (CallHandoverEvent) entry;
        System.out.println("HandleHandoverEvent " + call_hand_event.toString());

        int current_station = call_hand_event.current_station;
        int next_station = call_hand_event.next_station;

        // Handover from the previous Federate
        if (current_station == -1) {
            num_call_handover_from_other_fed++;
            current_station = index * NUM_BASE - 1;
            next_station = index * NUM_BASE;
        }

        // not a handover to next Federate
        if (next_station < (index + 1) * NUM_BASE) {
            num_call_handover++;
        }

        // free a channel
        if (current_station >= index * NUM_BASE
                && current_station < (index + 1) * NUM_BASE) {
            free_channels[current_station % NUM_BASE] += 1;
        }

        if (next_station >= index * NUM_BASE
                && next_station < (index + 1) * NUM_BASE) {
            if (free_channels[next_station % NUM_BASE] == 0) {
                num_dropped_call++;
                return true; // dropped call
            }
            free_channels[next_station % NUM_BASE] -= 1;
        }

        double remaining_duration = call_hand_event.remaining_duration;
        double _speed = call_hand_event.speed;
        if (next_station < (index + 1) * NUM_BASE) {

            if (RoundDouble(_speed * remaining_duration) < COVER_RANGE) {
                CallTermEvent call_term_ev = new CallTermEvent(EventTypes.CALL_TERMINATION,
                        RoundDouble(event_time + remaining_duration), next_station);

                event_list.add(call_term_ev);
            } else {
                GenerateNextHandover(_speed, event_time, next_station, region, remaining_duration);
            }
        }
        return false;
    }

    private void HandleCallTermEvent(Events entry) {
        
        CallTermEvent call_term_event = (CallTermEvent) entry;
        System.out.println("HandleHandoverEvent " + call_term_event.toString());
        free_channels[call_term_event.station % NUM_BASE] += 1;
    }

    private void GenerateNextHandover(double _speed, double event_time, int next_station, Region region, double remaining_duration) {
        // check handover other fed
        double elapsed_time = RoundDouble(COVER_RANGE / _speed);
        double next_event_time = RoundDouble(event_time + elapsed_time);
        double remain_call_time = RoundDouble(remaining_duration - elapsed_time);
        
        if (next_station + 1 >= (index + 1) * NUM_BASE) {
            if (index <= NUM_FED - 2) {
                try {
                    log("Before send " + fedamb.federateTime);
                    System.out.println("GenerateNextHandover " + Double.toString(next_event_time) + 
                            " " + Double.toString(_speed) + " " 
                            + Double.toString(remain_call_time));
                    // send interaction
                    sendInteraction(region, next_event_time, _speed, remain_call_time);

                } catch (RTIexception ex) {
                    Logger.getLogger(BaseStationFederate.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
        CallHandoverEvent call_hand_ev =
                new CallHandoverEvent(EventTypes.CALL_HANDOVER,
                next_event_time,
                _speed, next_station, next_station + 1,
                remain_call_time);
        event_list.add(call_hand_ev);
    }
}