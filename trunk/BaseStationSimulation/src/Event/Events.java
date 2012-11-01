/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Event;

/**
 *
 * @author pham0071
 */
public class Events implements Comparable<Events>{
    public Double event_time;
    public EventTypes event_type;    

    @Override
    public int compareTo(Events o) {
        return (int)(this.event_time - o.event_time);
    }
}
