package ir.ac.iust.dml.kg.log;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by ali on 17/02/17.
 */
public class LogRecord {
    private long freq;

    public Date getDate() {
        return date;
    }

    public long getFreq() {
        return freq;
    }

    public String getQuery() {
        return query;
    }

    private Date date;
    private String query;

    public static LogRecord ParseLine(String line) {
        LogRecord logRecord = new LogRecord();
        //System.out.println("line: " + line);
        try {
            if (StringUtils.countMatches(line, ",") < 2) {
                System.err.println("Line: \"" + line + "\" cannot be parsed (insufficient elements in line).");
                return null;
            }
            String[] splits = line.split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            logRecord.date =sdf.parse(splits[splits.length - 1]);
            logRecord.freq = Long.parseLong(splits[splits.length - 2]);
            logRecord.query = String.join("",(String[]) Arrays.copyOfRange(splits, 0, splits.length - 2));

        } catch (Exception e) {
            System.err.println("Line: \"" + line + "\" cannot be parsed");
            e.printStackTrace();
            return null;
        }
        return logRecord;
    }

    @Override
    public String toString() {
        return String.format("\"%s\"\tx %d\t %tF", query, freq, date);
    }
}
