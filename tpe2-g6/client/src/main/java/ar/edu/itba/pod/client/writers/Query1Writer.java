package ar.edu.itba.pod.client.writers;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Query1Writer extends QueryWriter {

    private static final String HEADER_NEIGHBORHOOD = "NEIGHBOURHOOD";
    private static final String HEADER_TREE_COUNT = "TREES";

    public Query1Writer(String outFilePath, String separator) {
        super(outFilePath, separator);
    }

    public Query1Writer(String outFilePath) {
        super(outFilePath);
    }

    public void writeQueryResults(List<Map.Entry<String, Double>> result) throws IOException {
        int queryId = 1;
        String fileName = String.format("%s/%s%d.csv", this.outFilePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_TREE_COUNT);
        writer.write(header);

        for (Map.Entry<String, Double> e : result) {
            String currentLine = String.format(lineStructure, e.getKey(), e.getValue().longValue());
            writer.append(currentLine);
        }

        writer.close();
    }
}
