package ar.edu.itba.pod.client;

public enum EventType {

    // possible values, is case sensitive
    FILE_READ_START("Inicio de la lectura del archivo"),
    FILE_READ_END("Fin de la lectura del archivo"),
    MAPREDUCE_START("Inicio del trabajo map/reduce"),
    MAPREDUCE_END("Fin del trabajo map/reduce");

    private final String message;

    // constructor
    EventType(String message) {
        this.message = message;
    }

    // getters
    public String getMessage() {
        return message;
    }
}
