package ar.edu.itba.pod.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeLogger {

    private String queryId;
    private String outFilePath;

    public TimeLogger(String queryId, String outFilePath) {
        this.queryId = queryId;
        this.outFilePath = outFilePath;
    }

    public void addEvent(EventType eventType) throws IOException {
        FileWriter file =  new FileWriter(outFilePath, true);
        BufferedWriter writer = new BufferedWriter(file);

        writer.append(String.format(
                "%s INFO %s - %s\n",
                new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSSS").format(new Date()),
                this.queryId,
                eventType.getMessage()
        ));

        writer.close();
    }
}
