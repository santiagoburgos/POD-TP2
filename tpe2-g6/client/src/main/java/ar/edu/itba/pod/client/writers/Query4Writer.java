package ar.edu.itba.pod.client.writers;

import ar.edu.itba.pod.api.model.PairedValues;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Query4Writer extends QueryWriter {

    // Headers
    private static final String HEADER_NEIGHBOURHOOD_PAIR_DISTINCT_TREE_COUNT = "GROUP";
    private static final String HEADER_NEIGHBOURHOOD_PAIR_FIRST = "NEIGHBOURHOOD A";
    private static final String HEADER_NEIGHBOURHOOD_PAIR_SECOND = "NEIGHBOURHOOD B";

    public Query4Writer(String outFilePath, String separator) {
        super(outFilePath, separator);
    }

    public Query4Writer(String outFilePath) {
        super(outFilePath);
    }

    public void writeQueryResults(List<PairedValues> result) throws IOException {
        int queryId = 4;
        String fileName = String.format("%s/%s%d.csv", outFilePath, QUERY_FILENAME, queryId);
        FileWriter file = new FileWriter(fileName, false);
        BufferedWriter writer = new BufferedWriter(file);

        String lineStructure = buildLineStructure(3);

        // header
        String header = String.format(lineStructure, HEADER_NEIGHBOURHOOD_PAIR_DISTINCT_TREE_COUNT, HEADER_NEIGHBOURHOOD_PAIR_FIRST, HEADER_NEIGHBOURHOOD_PAIR_SECOND);
        writer.write(header);

        // body
        for (PairedValues pv : result) {
            String currentLine = String.format(lineStructure, pv.getCommonValue(), pv.getMember1(), pv.getMember2());
            writer.append(currentLine);
        }

        writer.close();
    }
}
