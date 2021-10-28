package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
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

    public List<Tree> parseTrees(String filePath, String cityName) throws IOException {

        // get headers
        FileFormat currentFileFormat;
        try {
            currentFileFormat = FileFormat.valueOf(cityName);
        } catch (IllegalArgumentException e) {
            throw new NoSuchFileException(e.getMessage());
        }

        // return value
        List<Tree> trees = new ArrayList<>();

        // parse trees
        int currentLine = 0;

        String fileName = String.format("%s/%s%s.csv", filePath, TREES_FILE, cityName);
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

                Tree currentTree = new Tree(new Neighbourhood(currentNeighbourhoodName), currentStreetName, currentName);
                trees.add(currentTree);
            }

            // update line
            currentLine++;
        }

        return trees;
    }

    public List<Neighbourhood> parseNeighbourhoods(String filePath, String cityName) throws IOException {

        // get headers
        FileFormat currentFileFormat;
        try {
            currentFileFormat = FileFormat.valueOf(cityName);
        } catch (IllegalArgumentException e) {
            throw new NoSuchCityException();
        }

        List<Neighbourhood> neighbourhoods = new ArrayList<>();

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

                Neighbourhood currentNeighbourhood = new Neighbourhood(currentName, currentPopulation);
                neighbourhoods.add(currentNeighbourhood);
            }

            // update line
            currentLine++;
        }

        return neighbourhoods;
    }
}
