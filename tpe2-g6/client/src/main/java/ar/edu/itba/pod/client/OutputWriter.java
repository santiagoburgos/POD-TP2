package ar.edu.itba.pod.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OutputWriter {

    // File prefix
    private static final String QUERY_FILENAME = "query";

    // Headers
    private static final String HEADER_NEIGHBORHOOD = "NEIGHBOURHOOD";
    private static final String HEADER_TREE_COUNT = "TREES";
    private static final String HEADER_TREE_NAME = "COMMON_NAME";
    private static final String HEADER_TREES_PER_PERSON = "TREES_PER_PEOPLE";
    private static final String HEADER_DISTINCT_TREE_COUNT = "COMMON_NAME_COUNT";
    private static final String HEADER_NEIGHBOURHOOD_PAIR_DISTINCT_TREE_COUNT = "GROUP";
    private static final String HEADER_NEIGHBOURHOOD_PAIR_FIRST = "NEIGHBOURHOOD A";
    private static final String HEADER_NEIGHBOURHOOD_PAIR_SECOND = "NEIGHBOURHOOD B";
    private static final String HEADER_STREET_PAIR_DISTINCT_TREE_COUNT = "GROUP";
    private static final String HEADER_STREET_PAIR_FIRST = "STREET A";
    private static final String HEADER_STREET_PAIR_SECOND = "STREET B";


    // indicates what String to use as the file separator
    private String separator;

    public OutputWriter(String separator) {
        this.separator = separator;
    }

    public OutputWriter() {
        this(";");
    }

    // same function name with different parameters
    // for query 1
    // TODO add list of models as parameter List<Model>
    public void writeQueryResults(String filePath) throws IOException {
        int queryId = 1;
        String fileName = String.format("%s/%s%d.csv", filePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_TREE_COUNT);
        writer.write(header);

        // body
        /* TODO: uncomment on model definition & modify accordingly
        for(Model model : modelList){
            String currentLine = String.format(lineStructure, model.getNeighbourhood, model.getDistinctTreeCount);
            writer.append(currentLine);
        }
         */

        writer.close();
    }

    // for query 2
    // TODO add list of models as parameter List<Model>
    public void writeQueryResults(String filePath) throws IOException {
        int queryId = 2;
        String fileName = String.format("%s/%s%d.csv", filePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_TREE_NAME, HEADER_TREES_PER_PERSON);
        writer.write(header);

        // body
        /* TODO: uncomment on model definition & modify accordingly
        for(Model model : modelList){
            String currentLine = String.format(lineStructure, model.getNeighbourhood, model.getTree, model.getTreesPerPerson);
            writer.append(currentLine);
        }
         */

        writer.close();
    }

    // for query 3
    // TODO add list of models as parameter List<Model>
    public void writeQueryResults(String filePath) throws IOException {
        int queryId = 3;
        String fileName = String.format("%s/%s%d.csv", filePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_DISTINCT_TREE_COUNT);
        writer.write(header);

        // body
        /* TODO: uncomment on model definition & modify accordingly
        for(Model model : modelList){
            String currentLine = String.format(lineStructure, model.getNeighbourhood, model.getTree, model.getTreesPerPerson);
            writer.append(currentLine);
        }
         */

        writer.close();
    }

    // for query 4
    // TODO add list of models as parameter List<Model>
    public void writeQueryResults(String filePath) throws IOException {
        int queryId = 4;
        String fileName = String.format("%s/%s%d.csv", filePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(3);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBOURHOOD_PAIR_DISTINCT_TREE_COUNT, HEADER_NEIGHBOURHOOD_PAIR_FIRST, HEADER_NEIGHBOURHOOD_PAIR_SECOND);
        writer.write(header);

        // body
        /* TODO: uncomment on model definition & modify accordingly
        for(Model model : modelList){
            String currentLine = String.format(lineStructure, model.getPairDistinctTreeCount, model.getFirst, model.getSecond);
            writer.append(currentLine);
        }
         */

        writer.close();
    }

    // for query 5
    // TODO add list of models as parameter List<Model>
    public void writeQueryResults(String filePath) throws IOException {
        int queryId = 5;
        String fileName = String.format("%s/%s%d.csv", filePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(3);

        // header
        String header = String.format(lineStructure, HEADER_STREET_PAIR_DISTINCT_TREE_COUNT, HEADER_STREET_PAIR_FIRST, HEADER_STREET_PAIR_SECOND);
        writer.write(header);

        // body
        /* TODO: uncomment on model definition & modify accordingly
        for(Model model : modelList){
            String currentLine = String.format(lineStructure, model.getPairDistinctTreeCount, model.getFirst, model.getSecond);
            writer.append(currentLine);
        }
         */

        writer.close();
    }

    // auxiliar
    private String buildLineStructure(int elementCount) {
        StringBuffer returnValue = new StringBuffer();

        int currentElementCount = 0;
        while (currentElementCount < elementCount) {
            returnValue.append("%s");
            currentElementCount++;
            if (currentElementCount < elementCount)
                returnValue.append(this.separator);
            else
                returnValue.append('\n');
        }

        return returnValue.toString();
    }
}
