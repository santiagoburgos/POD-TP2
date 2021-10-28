package ar.edu.itba.pod.client.writers;

public abstract class QueryWriter {

    // File prefix
    protected static final String QUERY_FILENAME = "query";

    protected String outFilePath;
    // indicates what String to use as the file separator
    protected String separator;

    public QueryWriter(String outFilePath, String separator) {
        this.outFilePath = outFilePath;
        this.separator = separator;
    }

    public QueryWriter(String outFilePath) {
        this(outFilePath, ";");
    }

    protected String buildLineStructure(int elementCount) {
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
