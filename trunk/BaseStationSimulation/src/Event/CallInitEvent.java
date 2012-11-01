/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Event;

/**
 *
 * @author pham0071
 */
public class CallInitEvent extends Events {
    public double speed;
    public int station;
    public double position;
    public double duration;

    public CallInitEvent() {        
    }

    public CallInitEvent(EventTypes type, double event_time,
            double speed, int station, double position, double duration) {
        this.event_type = type;
        this.event_time = event_time;
        this.speed = speed;
        this.station = station;
        this.position = position;
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "CallInitEvent{" + "speed=" + speed + ", station=" + station + ", position=" + position + ", duration=" + duration + '}';
    }
    
    
    
}
