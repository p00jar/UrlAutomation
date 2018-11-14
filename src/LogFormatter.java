import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class LogFormatter extends Formatter {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public LogFormatter() {
    }

    public String format(LogRecord record) {
        String message = this.formatMessage(record);
        long time = record.getMillis();
        Throwable thrown = record.getThrown();
        SimpleDateFormat dateFormatter = new SimpleDateFormat("[HH:mm:ss:SSS]|[MM-dd-yyyy]");
        StringBuilder buf = new StringBuilder();
        buf.append(dateFormatter.format(time));
        buf.append("|");
        buf.append(message);
        if (thrown != null) {
            buf.append(" ").append(LINE_SEPARATOR);
            StringWriter sw = new StringWriter(1024);
            PrintWriter pw = new PrintWriter(sw);
            thrown.printStackTrace(pw);
            pw.close();
            buf.append(sw.toString());
        }

        buf.append(LINE_SEPARATOR);
        return buf.toString();
    }
}

