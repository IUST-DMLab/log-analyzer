package ir.ac.iust.dml.kg.log;

import ir.ac.iust.dml.kg.entity.extractor.MatchedEntity;

import java.util.List;

/**
 * Created by ali on 17/02/17.
 */
public class QueryRecord extends Query {
    private long freq;

    public long getFreq() {
        return freq;
    }

    public QueryRecord(long freq, String queryText) {
        super(queryText);
        this.freq = freq;
    }

    @Override
    public String toString() {return String.format("\"%s\"\tx %d", queryText, freq);}
}
