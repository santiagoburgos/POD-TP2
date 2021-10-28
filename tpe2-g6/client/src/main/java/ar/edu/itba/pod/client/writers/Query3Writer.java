package ar.edu.itba.pod.client.writers;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Query3Writer extends QueryWriter {

    // Headers
    private static final String HEADER_NEIGHBORHOOD = "NEIGHBOURHOOD";
    private static final String HEADER_DISTINCT_TREE_COUNT = "COMMON_NAME_COUNT";

    public Query3Writer(String outFilePath, String separator) {
        super(outFilePath, separator);
    }

    public Query3Writer(String outFilePath) {
        super(outFilePath);
    }


    public void writeQueryResults(List<Map.Entry<String, Long>> result) throws IOException {
        int queryId = 3;
        String fileName = String.format("%s/%s%d.csv", outFilePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(2);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBORHOOD, HEADER_DISTINCT_TREE_COUNT);
        writer.write(header);

        for (Map.Entry<String, Long> e : result) {
            String currentLine = String.format(lineStructure, e.getKey(), e.getValue());
            writer.append(currentLine);
        }

        writer.close();
    }
}
