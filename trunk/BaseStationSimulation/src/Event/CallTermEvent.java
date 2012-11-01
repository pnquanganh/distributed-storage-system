/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Event;

/**
 *
 * @author pham0071
 */
public class CallTermEvent extends Events{
    public int station;

    public CallTermEvent() {
    }

    public CallTermEvent(EventTypes type, double event_time, int station) {
        this.event_type = type;
        this.event_time = event_time;
        this.station = station;
    }

    @Override
    public String toString() {
        return "CallTermEvent{" + "station=" + station + '}';
    }
    
}
