package ar.edu.itba.pod.api.model;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;


public class PairCompoundKeyValue implements DataSerializable {
    private String k1;
    private String k2;
    private Double value;

    protected PairCompoundKeyValue(){}

    public PairCompoundKeyValue(String k1, String k2, Double value) {
        this.k1 = k1;
        this.value = value;
        this.k2 = k2;
    }

    public String getK1() {
        return k1;
    }
    public void setK1(String neighbourhood) {
        this.k1 = neighbourhood;
    }
    public String getK2() {
        return k2;
    }
    public void setK2(String name) {
        this.k2 = name;
    }
    public Double getValue(){
        return value;
    }
    public void setValue(Double v) {
        this.value = v;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.k1);
        objectDataOutput.writeUTF(this.k2);
        objectDataOutput.writeDouble(this.value);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.k1 = objectDataInput.readUTF();
        this.k2 = objectDataInput.readUTF();
        this.value = objectDataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairCompoundKeyValue other = (PairCompoundKeyValue) o;
        return this.k1.equals(other.k1) && this.k2.equals(other.k2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k1, k2);
    }


}
