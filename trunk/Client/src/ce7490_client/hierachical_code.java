/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ce7490_client;

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

    private byte[] xor(byte[] op1, byte[] op2) {
        byte[] result = new byte[op1.length];
        for (int i = 0; i < op1.length; i++) {
            result[i] = (byte) (op1[i] ^ op2[i]);
        }
        return result;
    }

    public void encode(byte[] data) {
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
        System.arraycopy(data, 2 * code_size, o3, 0, code_size);
        System.arraycopy(data, 3 * code_size, o4, 0, code_size);

//        o1o2 = new byte[code_size];
//        o3o4 = new byte[code_size];
//        o1o2o3o4 = new byte[code_size];

        o1o2 = xor(o1, o2);
        o3o4 = xor(o3, o4);
        o1o2o3o4 = xor(o1o2, o3o4);

    }

    private byte[] recover_data(byte[] full_data, byte[] data11, byte[] data12, byte[] data2) {

        if (data11 == null && data12 != null && data2 != null) {
            data11 = xor(data12, data2);
        }

        return xor(full_data, data11);

    }

    public byte[] decode() {
        if ((o1 == null && o2 == null) || (o3 == null && o4 == null)) {
            return null;
        }

        byte[] lost_data = null;
        byte[] full_data = null;
        byte[] data11 = null;
        byte[] data12 = null;
        byte[] data2 = o1o2o3o4;

        if (o1 == null || o2 == null) {
            if (o1 == null) {
                //lost_data = o1;
                full_data = o2;
                data11 = o1o2;
                data12 = o3o4;
                o1 = recover_data(full_data, data11, data12, data2);

            }
            if (o2 == null) {
                lost_data = o2;
                full_data = o1;
                data11 = o1o2;
                data12 = o3o4;
                o2 = recover_data(full_data, data11, data12, data2);
            }
            
        }

        if (o3 == null || o4 == null) {
            if (o3 == null) {
                lost_data = o3;
                full_data = o4;
                data11 = o3o4;
                data12 = o1o2;
                o3 = recover_data(full_data, data11, data12, data2);
            }
            if (o4 == null) {
                lost_data = o4;
                full_data = o3;
                data11 = o3o4;
                data12 = o1o2;
                o4 = recover_data(full_data, data11, data12, data2);
            }
            
        }


        int code_size = o1.length;
        byte[] result = new byte[4 * code_size];

        System.arraycopy(o1, 0, result, 0, code_size);
        System.arraycopy(o2, 0, result, code_size, code_size);
        System.arraycopy(o3, 0, result, 2 * code_size, code_size);
        System.arraycopy(o4, 0, result, 3 * code_size, code_size);

        return result;
    }
}
