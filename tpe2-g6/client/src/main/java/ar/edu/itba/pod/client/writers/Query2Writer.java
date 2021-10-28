package ar.edu.itba.pod.client.writers;

import ar.edu.itba.pod.api.model.PairCompoundKeyValue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Query2Writer extends QueryWriter {

    // Headers
    private static final String HEADER_NEIGHBORHOOD = "NEIGHBOURHOOD";
    private static final String HEADER_TREE_NAME = "COMMON_NAME";
    private static final String HEADER_TREES_PER_PERSON = "TREES_PER_PEOPLE";

    public Query2Writer(String outFilePath, String separator) {
        super(outFilePath, separator);
    }

    public Query2Writer(String outFilePath) {
        super(outFilePath);
    }

    public void writeQueryResults(List<Map.Entry<String, PairCompoundKeyValue>> result) throws IOException {
        int queryId = 2;
        String fileName = String.format("%s/%s%d.csv", outFilePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_TREE_NAME, HEADER_TREES_PER_PERSON);
        writer.write(header);

        // body
        for (Map.Entry<String, PairCompoundKeyValue> e : result) {
            String currentLine = String.format(lineStructure, e.getValue().getK1(), e.getValue().getK2(), String.format(Locale.ROOT, "%.2f", e.getValue().getValue()));
            writer.append(currentLine);
        }

        writer.close();
    }
}
