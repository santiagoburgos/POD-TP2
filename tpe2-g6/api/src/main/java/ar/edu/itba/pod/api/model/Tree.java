package ar.edu.itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Tree implements DataSerializable {
    private String neighbourhood;
    private String street;
    private String name;

    protected Tree(){}

    public Tree(String neighbourhood, String street, String name) {
        this.neighbourhood = neighbourhood;
        this.street = street;
        this.name = name;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public void setNeighbourhood(String neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.neighbourhood);
        objectDataOutput.writeUTF(this.street);
        objectDataOutput.writeUTF(this.name);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.neighbourhood = objectDataInput.readUTF();
        this.street = objectDataInput.readUTF();
        this.name = objectDataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tree tree = (Tree) o;
        return neighbourhood.equals(tree.neighbourhood) &&
                street.equals(tree.street) &&
                name.equals(tree.name);
    }
}
