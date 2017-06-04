package upem.jarret.server;

import java.io.IOException;

public class HTTPServerException extends IOException {

    private static final long serialVersionUID = -1810727803680020453L;

    public HTTPServerException() {
        super();
    }

    public HTTPServerException(String s) {
        super(s);
    }

    public static void ensure(boolean b, String string) throws HTTPServerException {
        if (!b)
            throw new HTTPServerException(string);
    }
}