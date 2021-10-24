package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.exceptions.MissingFieldException;
import ar.edu.itba.pod.client.exceptions.NoSuchCityException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;

public class FileParser {

    private static final String BUE_CODE = "BUE";
    private static final String VAN_CODE = "VAN";

    private static final String TREES_FILE = "arboles";
    private static final String NEIGHBOURHOODS_FILE = "barrios";

    // indicates what String to use as the file separator
    private String separator;

    public FileParser(String separator) {
        this.separator = separator;
    }

    public FileParser() {
        this(";");
    }

    // TODO: Change return type to List<[TREE MODEL]>
    public void parseTrees(String filePath, String cityName) throws IOException {

        // get headers
        FileFormat currentFileFormat;
        try {
            currentFileFormat = FileFormat.valueOf(cityName);
        } catch (IllegalArgumentException e) {
            throw new NoSuchFileException(e.getMessage());
        }

        // TODO uncomment when model is ready
        // return value
        // List<Tree> trees = new ArrayList<>();

        // parse trees
        int currentLine = 0;

        String fileName = String.format("%s/%s%s.csv", filePath, NEIGHBOURHOODS_FILE, cityName);
        Path path = Paths.get(fileName);
        BufferedReader file = Files.newBufferedReader(path);

        String line;
        Integer neighbourhoodNameIndex = null;
        Integer streetNameIndex = null;
        Integer nameIndex = null;
        while ((line = file.readLine()) != null) {

            String[] parts = line.split(this.separator);

            if (currentLine == 0) {
                // analyse header in first line
                for (int i = 0; i < parts.length; i++) {

                    if (neighbourhoodNameIndex == null && parts[i].trim().equals(currentFileFormat.getTreesNeighbourhoodName()))
                        neighbourhoodNameIndex = i;

                    if (streetNameIndex == null && parts[i].trim().equals(currentFileFormat.getTreesStreetName()))
                        streetNameIndex = i;

                    if (nameIndex == null && parts[i].trim().equals(currentFileFormat.getTreesName()))
                        nameIndex = i;
                }
                // when there are lacking headers, shouldn't happen for provided cases
                if (neighbourhoodNameIndex == null || streetNameIndex == null || nameIndex == null)
                    throw new MissingFieldException();
            } else {
                // analyse other lines
                String currentNeighbourhoodName = parts[neighbourhoodNameIndex].trim();
                String currentStreetName = parts[streetNameIndex].trim();
                String currentName = parts[nameIndex].trim();

                // TODO uncomment when model is ready
                /*
                currentTree = new Tree(currentNeighbourhoodName, currentStreetName, currentName)
                trees.append(currentTree)
                 */
            }

            // update line
            currentLine++;
        }

        // TODO uncomment when model is ready
        // return trees;
    }

    // TODO: Change return type to List<[NEIGHBOURHOOD MODEL]>
    public void parseNeighbourhoods(String filePath, String cityName) throws IOException {

        // get headers
        FileFormat currentFileFormat;
        try {
            currentFileFormat = FileFormat.valueOf(cityName);
        } catch (IllegalArgumentException e) {
            throw new NoSuchCityException();
        }

        // TODO uncomment when model is ready
        // List<Neighbourhood> neighbours = new ArrayList<>();

        // parse neighbours
        int currentLine = 0;

        String fileName = String.format("%s/%s%s.csv", filePath, NEIGHBOURHOODS_FILE, cityName);
        Path path = Paths.get(fileName);
        BufferedReader file = Files.newBufferedReader(path);

        String line;
        Integer nameIndex = null;
        Integer populationIndex = null;
        while ((line = file.readLine()) != null) {

            String[] parts = line.split(this.separator);

            if (currentLine == 0) {
                // analyse header in first line
                for (int i = 0; i < parts.length; i++) {
                    if (nameIndex == null && parts[i].trim().equals(currentFileFormat.getNeighbourhoodNeighbourhoodName()))
                        nameIndex = i;

                    if (populationIndex == null && parts[i].trim().equals(currentFileFormat.getNeighbourhoodPopulation()))
                        populationIndex = i;
                }
                // when there are lacking headers, shouldn't happen for provided cases
                if (nameIndex == null || populationIndex == null)
                    throw new MissingFieldException();
            } else {
                // analyse other lines
                String currentName = parts[nameIndex].trim();
                long currentPopulation = Long.parseLong(parts[populationIndex].trim());

                // TODO uncomment when model is ready
                /*
                currentNeighbourhood = new Neighbourhood(currentName, currentPopulation)
                neighbourhood.append(currentNeighbourhood)
                 */
            }

            // update line
            currentLine++;
        }

        // TODO uncomment when model is ready
        // return neighbours;
    }
}
