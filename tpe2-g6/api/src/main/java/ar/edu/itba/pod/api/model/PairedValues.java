package ar.edu.itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class PairedValues implements Comparable<PairedValues>, DataSerializable {
    private Long commonValue;
    private String member1;
    private String member2;

    protected PairedValues(){};

    // Precaution, this auxiliary object does not consider case member1 == member2, this should be avoided by the user
    public PairedValues(Long commonValue, String member1, String member2) {
        this.commonValue = commonValue;
        final boolean is1LowerThan2 = member1.toLowerCase().compareTo(member2.toLowerCase()) < 0;
        this.member1 = is1LowerThan2 ? member1 : member2;
        this.member2 = is1LowerThan2 ? member2 : member1;
    }

    @Override
    public int compareTo(PairedValues pairedValues) {
        final int cvc = commonValue.compareTo(pairedValues.commonValue);
        if (cvc != 0) return cvc * -1;
        final int m1c = member1.compareTo(pairedValues.member1);
        if (m1c != 0) return m1c;
        return member2.compareTo(pairedValues.member2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairedValues that = (PairedValues) o;
        return Objects.equals(commonValue, that.commonValue) && Objects.equals(member1, that.member1) && Objects.equals(member2, that.member2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonValue, member1, member2);
    }

    public Long getCommonValue() {
        return commonValue;
    }

    public String getMember1() {
        return member1;
    }

    public String getMember2() {
        return member2;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeLong(commonValue);
        objectDataOutput.writeUTF(member1);
        objectDataOutput.writeUTF(member2);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.commonValue = objectDataInput.readLong();
        this.member1 = objectDataInput.readUTF();
        this.member2 = objectDataInput.readUTF();
    }
}
