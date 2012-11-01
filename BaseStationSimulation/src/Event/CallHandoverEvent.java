/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Event;

/**
 *
 * @author pham0071
 */
public class CallHandoverEvent extends Events{
    

    public double speed;
    public int current_station;
    public int next_station;
    public double remaining_duration;

    public CallHandoverEvent() {
    }

    public CallHandoverEvent(EventTypes type, double event_time,
            double speed, int current_station,
            int next_station, double remaining_duration) {
        this.event_type = type;
        this.event_time = event_time;
        this.speed = speed;
        this.current_station = current_station;     
        this.next_station = next_station;

        this.remaining_duration = remaining_duration;
    }

    @Override
    public String toString() {
        return "CallHandoverEvent{" + "speed=" + speed + ", next_station=" + next_station + ", remaining_duration=" + remaining_duration + '}';
    }
    
    
}
