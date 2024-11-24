package com.ucas.bigdata.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum MetaOpCode {
    HEART_BEAT((byte)30),
    CREATE_FILE((byte)31),
    RENAME_FILE((byte)32),
    DEL_FILE((byte)33),
    LIST_FILE((byte)34),
    GET_FILE_LOCATIONS((byte)35),
    CLOSE_FILE((byte)36);




    public final byte code;

    private MetaOpCode(byte code) {
        this.code = code;
    }

    private static final int FIRST_CODE = values()[0].code;

    private static MetaOpCode valueOf(byte code) {
        final int i = (code & 0xff) - FIRST_CODE;
        return i < 0 || i >= values().length? null: values()[i];
    }

    public static MetaOpCode read(DataInput in) throws IOException {
        return valueOf(in.readByte());
    }

    public void write(DataOutput out) throws IOException {
        out.writeByte(code);
    }
}
