package command;

import java.io.IOException;

public interface Handler {

    byte[] handle(String[] args) throws IOException;
}
