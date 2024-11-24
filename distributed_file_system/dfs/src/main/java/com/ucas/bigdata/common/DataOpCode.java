package com.ucas.bigdata.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum DataOpCode {
    WRITE_FILE((byte)10),
    READ_FILE((byte)11),
    DEL_FILE((byte)12);

    public final byte code;

    private DataOpCode(byte code) {
        this.code = code;
    }

    private static final int FIRST_CODE = values()[0].code;

    private static DataOpCode valueOf(byte code) {
        final int i = (code & 0xff) - FIRST_CODE;
        return i < 0 || i >= values().length? null: values()[i];
    }

    public static DataOpCode read(DataInput in) throws IOException {
        return valueOf(in.readByte());
    }

    public void write(DataOutput out) throws IOException {
        out.write(code);
    }
}
