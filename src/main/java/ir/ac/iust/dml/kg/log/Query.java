package ir.ac.iust.dml.kg.log;

import ir.ac.iust.dml.kg.entity.extractor.MatchedEntity;

import java.util.List;

/**
 * Created by ali on 17/02/17.
 */
public class Query {
    protected String queryText;
    protected List<MatchedEntity> matchedEntities;

    public Query(String queryText) {
        this.queryText = queryText;
    }

    public String getQueryText() {
        return queryText;
    }
    public List<MatchedEntity> getMatchedEntities() {
        return matchedEntities;
    }
    public void setMatchedEntities(List<MatchedEntity> matchedEntities) {
        this.matchedEntities = matchedEntities;
    }

    public String toString() {return String.format("\"%s\"\tx %d", queryText);}
}
