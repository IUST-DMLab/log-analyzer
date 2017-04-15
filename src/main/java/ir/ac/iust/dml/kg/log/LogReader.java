package ir.ac.iust.dml.kg.log;

import ir.ac.iust.dml.kg.entity.extractor.IEntityExtractor;
import ir.ac.iust.dml.kg.entity.extractor.IEntityReader;
import ir.ac.iust.dml.kg.entity.extractor.MatchedEntity;
import ir.ac.iust.dml.kg.entity.extractor.readers.EntityReaderFromAllJson;
import ir.ac.iust.dml.kg.entity.extractor.readers.EntityReaderFromRedirectJson;
import ir.ac.iust.dml.kg.entity.extractor.tree.TreeEntityExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by ali on 17/02/17.
 */
public class LogReader {
    static final Logger LOGGER = LoggerFactory.getLogger(LogReader.class);
    private static Properties properties = new Properties();

    public static void main(String[] args) throws Exception {
        try {
            properties.load(LogReader.class.getResourceAsStream("/config.properties"));
        }catch(Exception e){ LOGGER.error("Cannot load configuration file", e); }

        String logFileName = properties.getProperty("log.file.path","data/queries_filtered.csv");
        IEntityExtractor extractor = setupNewExtractor();

        //Extract queries with Freq
        List<QueryRecord> queryRecords = Files.lines(Paths.get(logFileName))
                .parallel()
                .skip(1)
                .map(l -> LogRecordParser.ParseLine(l))
                .filter(lr -> lr != null)
                .collect(Collectors.toList());

        //Extract queriesFreq
        Map<String,Long> queriesFreq = new HashMap<>();
        queryRecords.forEach(lr -> queriesFreq.put(lr.getQueryText(), lr.getFreq()+queriesFreq.getOrDefault(lr.getQueryText(),0l)));
        Utils.persistSortedMap(queriesFreq, "results/queriesFreq.txt");

        Map<String,Long> typesFreq = new HashMap<>();
        Map<String,Long> typesFreqOfNonValidTypes = new HashMap<>();

        BufferedWriter writer = Files.newBufferedWriter(Paths.get("results/analysis.txt"));

        for(QueryRecord lr : queryRecords){
            try {
                /*lr.setMatchedEntities(extractor.search(lr.getQueryText(), true));*/
                writer.write("\n\n QUERY:\t" + lr.getQueryText() + "\n");
                List<MatchedEntity> result = extractor.search(lr.getQueryText(), true);
                for (MatchedEntity mQ : result){
                    writer.write("\tENTITY:\t" + mQ.toString()+ "\n");

                    if(mQ.getClassTree() == null)
                        writer.write("\t\tCLASS:\tgetClassTree() is NULL\n");
                    else {
                        for (String cls : mQ.getClassTree()) {
                            writer.write("\t\tCLASS:\t" + cls + "\n");
                            typesFreq.put(cls, lr.getFreq() + typesFreq.getOrDefault(cls, 0l));
                            if(!Arrays.stream(mQ.getClassTree()).anyMatch(a -> a.equals("Thing")))
                                typesFreqOfNonValidTypes.put(cls, lr.getFreq() + typesFreqOfNonValidTypes.getOrDefault(cls, 0l));
                        }
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
                throw e;
            }
        }

        writer.close();
        Utils.persistSortedMap(typesFreq, "results/typesFreq.txt");
        Utils.persistSortedMap(typesFreqOfNonValidTypes, "results/typesFreqOfNonValidTypes.txt");

        /*Map<String,Long> entitiesFreq = new HashMap<>();
        int num=0;
        for(QueryRecord lR : queryRecords)
            num += lR.getMatchedEntities().size();
            //System.out.println(lR.getQueryText() + " ==> " + lR.getMatchedEntities().size());
            //lR.getMatchedEntities().forEach(entity -> entitiesFreq.put(entity.getEntity(), lR.getFreq() + entitiesFreq.getOrDefault(entitiesFreq.get(entity.getEntity()),0l)));
            //lR.getMatchedEntities().forEach(System.out::println);
        System.out.println("total: " + num);


        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("Entity_freqs.txt"))) {
            for(Map.Entry<String,Long> pair : entitiesFreq.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).collect(Collectors.toList()))
                writer.write(pair.getKey() + "\t" + pair.getValue() + "\n");
        }*/



        /*try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("Queries_with_Entities.txt"))) {

            Iterator<Map.Entry<String, Long>> itr = queriesFreq.entrySet()
                    .stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()).iterator();

            while(itr.hasNext()){
                Map.Entry<String, Long> e = itr.next();
                writer.write(String.format("Query: \"%s\" x %d times\n", e.getKey(), e.getValue()));

                List<Entity> matchedEntities = extractor.search(e.getKey(),true);
                for (Entity detectedEntity : matchedEntities) {
                    entitiesFreq.put(detectedEntity.toString(), entitiesFreq.getOrDefault(detectedEntity.toString(),0l) + e.getValue());
                    writer.write(String.format("\tEntity: %s\n", detectedEntity.getEntity()));

                }
                writer.write("\n");

                queriesFreq.entrySet()
                        .stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed());

            }
        }*/
    }

    private static IEntityExtractor setupNewExtractor() throws Exception {
        IEntityExtractor extractor = new TreeEntityExtractor();
        try (IEntityReader reader = new EntityReaderFromAllJson("data/exportTypes")) {
            extractor.setup(reader, 3);
        }
        try (IEntityReader reader = new EntityReaderFromRedirectJson("data/redirects_map.json")) {
            extractor.setup(reader, 3);
        }
        return extractor;
    }


}