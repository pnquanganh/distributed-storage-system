/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hierachical_code;


/**
 *
 * @author pham0071
 */
public class hierachical_code {

    private byte[] o1 = null;
    private byte[] o2 = null;
    private byte[] o3 = null;
    private byte[] o4 = null;
    private byte[] o1o2 = null;
    private byte[] o3o4 = null;
    private byte[] o1o2o3o4 = null;

    public hierachical_code() {
    }

    ;

    
    /**
     * @return the o1
     */
    public byte[] getO1() {
        return o1;
    }

    /**
     * @return the o2
     */
    public byte[] getO2() {
        return o2;
    }

    /**
     * @return the o3
     */
    public byte[] getO3() {
        return o3;
    }

    /**
     * @return the o4
     */
    public byte[] getO4() {
        return o4;
    }

    /**
     * @param o1 the o1 to set
     */
    public void setO1(byte[] o1) {
        this.o1 = o1;
    }

    /**
     * @param o2 the o2 to set
     */
    public void setO2(byte[] o2) {
        this.o2 = o2;
    }

    /**
     * @param o3 the o3 to set
     */
    public void setO3(byte[] o3) {
        this.o3 = o3;
    }

    /**
     * @param o4 the o4 to set
     */
    public void setO4(byte[] o4) {
        this.o4 = o4;
    }

    /**
     * @return the o1o2
     */
    public byte[] getO1O2() {
        return o1o2;
    }

    /**
     * @param o1o2 the o1o2 to set
     */
    public void setO1O2(byte[] o1o2) {
        this.o1o2 = o1o2;
    }

    /**
     * @return the o3o4
     */
    public byte[] getO3O4() {
        return o3o4;
    }

    /**
     * @param o3o4 the o3o4 to set
     */
    public void setO3O4(byte[] o3o4) {
        this.o3o4 = o3o4;
    }

    /**
     * @return the o1o2o3o4
     */
    public byte[] getO1O2O3O4() {
        return o1o2o3o4;
    }

    /**
     * @param o1o2o3o4 the o1o2o3o4 to set
     */
    public void setO1O2O3O4(byte[] o1o2o3o4) {
        this.o1o2o3o4 = o1o2o3o4;
    }

    public void Encode(byte[] data) {
        if (data.length % 4 != 0) {
            int new_length = (int) (Math.ceil(data.length / (float) 4) * 4);
            byte[] _data = new byte[new_length];
            System.arraycopy(data, 0, _data, 0, data.length);
            data = _data;
        }

        int code_size = data.length / 4;

        o1 = new byte[code_size];
        o2 = new byte[code_size];
        o3 = new byte[code_size];
        o4 = new byte[code_size];

        System.arraycopy(data, 0, o1, 0, code_size);
        System.arraycopy(data, code_size, o2, 0, code_size);
        System.arraycopy(data, 2*code_size, o3, 0, code_size);
        System.arraycopy(data, 3*code_size, o4, 0, code_size);
        
        o1o2 = new byte[code_size];
        o3o4 = new byte[code_size];
        o1o2o3o4 = new byte[code_size];

        for (int i = 0; i < code_size; i++) {
            o1o2[i] = (byte) (o1[i] ^ o2[i]);
            o3o4[i] = (byte) (o3[i] ^ o4[i]);
            o1o2o3o4[i] = (byte) (o1o2[i] ^ o3o4[i]);
        }
    }
}
