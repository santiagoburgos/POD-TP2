package ar.edu.itba.pod.client;

public enum FileFormat {

    // possible values, is case sensitive
    VAN("NEIGHBOURHOOD_NAME", "STD_STREET", "COMMON_NAME", "nombre", "habitantes"),
    BUE("comuna", "calle_nombre", "nombre_cientifico", "nombre", "habitantes");

    // variables
    // tree related
    private final String treesNeighbourhoodName;
    private final String treesStreetName;
    private final String treesName;

    // neighbourhood related
    private final String neighbourhoodNeighbourhoodName;
    private final String neighbourhoodPopulation;

    // constructor
    FileFormat(String treesNeighbourhoodName, String treesStreetName, String treesName, String neighbourhoodNeighbourhoodName, String neighbourhoodPopulation) {
        this.treesNeighbourhoodName = treesNeighbourhoodName;
        this.treesStreetName = treesStreetName;
        this.treesName = treesName;
        this.neighbourhoodNeighbourhoodName = neighbourhoodNeighbourhoodName;
        this.neighbourhoodPopulation = neighbourhoodPopulation;
    }

    // getters
    public String getTreesNeighbourhoodName() {
        return treesNeighbourhoodName;
    }

    public String getTreesStreetName() {
        return treesStreetName;
    }

    public String getTreesName() {
        return treesName;
    }

    public String getNeighbourhoodNeighbourhoodName() {
        return neighbourhoodNeighbourhoodName;
    }

    public String getNeighbourhoodPopulation() {
        return neighbourhoodPopulation;
    }
}
