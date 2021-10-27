package ar.edu.itba.pod.api;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;


public class OutTreeNeighbourhood implements DataSerializable {
    private String neighbourhoodName;
    private String treeName;
    private Double population;

    public OutTreeNeighbourhood(){
    }


    public OutTreeNeighbourhood(String neighbourhoodName, String treeName, Double value) {
        this.neighbourhoodName = neighbourhoodName;
        this.population = value;
        this.treeName = treeName;
    }

    public String getNeighbourhoodName() {
        return neighbourhoodName;
    }

    public void setNeighbourhoodName(String neighbourhood) {
        this.neighbourhoodName = neighbourhood;
    }

    public String getTreeName() {
        return treeName;
    }

    public void setTreeName(String name) {
        this.treeName = name;
    }

    public Double getPopulation(){
        return population;
    }

    public void setPopulation(Double v) {
        this.population = v;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.neighbourhoodName);
        objectDataOutput.writeUTF(this.treeName);
        objectDataOutput.writeDouble(this.population);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.neighbourhoodName = objectDataInput.readUTF();
        this.treeName = objectDataInput.readUTF();
        this.population = objectDataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutTreeNeighbourhood other = (OutTreeNeighbourhood) o;
        return this.neighbourhoodName.equals(other.neighbourhoodName) && this.treeName.equals(other.treeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbourhoodName, treeName);
    }


}
