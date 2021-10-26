package ar.edu.itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class Neighbourhood implements DataSerializable {
    private String name;
    private Long population;

    public Neighbourhood(String name, Long population) {
        this.name = name;
        this.population = population;
    }

    public Neighbourhood(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.name);
        objectDataOutput.writeLong(this.population);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.name = objectDataInput.readUTF();
        this.population = objectDataInput.readLong();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Neighbourhood that = (Neighbourhood) obj;
        return Objects.equals(name, that.name);
    }
}
