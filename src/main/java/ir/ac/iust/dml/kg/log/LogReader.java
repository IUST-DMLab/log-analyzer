package ir.ac.iust.dml.kg.log;

import com.google.common.util.concurrent.AtomicLongMap;
import ir.ac.iust.dml.kg.resource.extractor.IResourceExtractor;
import ir.ac.iust.dml.kg.resource.extractor.IResourceReader;
import ir.ac.iust.dml.kg.resource.extractor.MatchedResource;
import ir.ac.iust.dml.kg.resource.extractor.Resource;
import ir.ac.iust.dml.kg.resource.extractor.readers.ResourceReaderFromKGStoreV1Service;
import ir.ac.iust.dml.kg.resource.extractor.tree.TreeResourceExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
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
        } catch (Exception e) {
            System.err.println("Cannot load configuration file: " + e.getStackTrace());
        }

        String logFileName = properties.getProperty("log.file.path", "data/queries_filtered.csv");
        IResourceExtractor extractor = setupNewExtractor();


        //Extract queries with Freq
        List<QueryRecord> queryRecords = Files.lines(Paths.get(logFileName))
                .parallel()
                .skip(1)
                .map(l -> LogRecordParser.ParseLine(l))
                .filter(lr -> lr != null)
                .collect(Collectors.toList());


        //Extract queriesFreq
        Map<String, Long> queriesFreq = new HashMap<>();
        queryRecords.forEach(lr -> queriesFreq.put(lr.getQueryText(), lr.getFreq() + queriesFreq.getOrDefault(lr.getQueryText(), 0l)));
        Utils.persistSortedMap(queriesFreq, "results/queriesFreq.txt");


        Map<String, Long> classFreqs = new HashMap<>();
        Map<String, Long> classFreqsOfNonMappedClasses = new HashMap<>();

        BufferedWriter writer = Files.newBufferedWriter(Paths.get("results/analysis.txt"));

        AtomicLongMap<String> entityPropertyFreqs = AtomicLongMap.create();

        for (QueryRecord lr : queryRecords) {
            try {
                    /*lr.setMatchedEntities(extractor.search(lr.getQueryText(), true));*/
                writer.write("\n\n QUERY:\t" + lr.getQueryText() + "\n");
                List<MatchedResource> result = extractor.search(lr.getQueryText(), true);
                if (result.size() > 0)
                    System.out.printf("%s:\t result size %d\n", lr.getQueryText(), result.size());
                for (MatchedResource mR : result) {
                    if (mR.getResource() == null) continue;
                    Resource mainResource = mR.getResource();

                    writer.write("\tResource label:\t" + noD(mainResource.getLabel()) + "\n");
                    writer.write("\t\tiri:\t" + noD(mainResource.getIri()) + "\n");
                    writer.write("\t\tinstanceOf:\t" + noD(mainResource.getInstanceOf()) + "\n");
                    writer.write("\t\tclassTree:\t" + noD(mainResource.getClassTree()) + "\n");
                    writer.write("\t\tvariantLabel:\t" + noD(mainResource.getVariantLabel()) + "\n");
                    writer.write("\t\tdisambiguatedFrom:\t" + noD(mainResource.getDisambiguatedFrom()) + "\n");
                    writer.write("\t\tType:\t" + noD(mainResource.getType()) + "\n");

                    Set<String> propertyClassTrees = new HashSet<>();
                    Set<String> entityClassTrees = new HashSet<>();

                    if (mR.getResource().getClassTree() != null) {
                        for (String cls : mR.getResource().getClassTree()) {
                            classFreqs.put(cls, lr.getFreq() + classFreqs.getOrDefault(cls, 0l));
                            //if(!Arrays.stream(mQ.getClassTree()).anyMatch(a -> a.equals("Thing")))
                            //    typesFreqOfNonValidTypes.put(cls, lr.getFreq() + typesFreqOfNonValidTypes.getOrDefault(cls, 0l));

                            //Counting patterns
                            if (mR.getResource().getType() != null) {
                                String type = clean(mR.getResource().getType().toString(), "#");
                                if (type.equals("Property"))
                                    propertyClassTrees.add(clean(cls, "/"));
                                else if (type.equals("Resource"))
                                    entityClassTrees.add(clean(cls, "/"));

                            }
                        }
                    }
                    for (String entityClass : entityClassTrees)
                        for (String propertyClass : propertyClassTrees)
                            entityPropertyFreqs.addAndGet(entityClass + "," + propertyClass, lr.getFreq());
                }


            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        writer.close();
        Utils.persistSortedMap(classFreqs, "results/classesFreq.txt");
        Utils.persistSortedMap(entityPropertyFreqs.asMap(), "results/patternsFreq.txt");

        //        Utils.persistSortedMap(typesFreqOfNonValidTypes, "results/typesFreqOfNonValidTypes.txt");

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
.

                }
            }*/
    }

    private static IResourceExtractor setupNewExtractor() throws Exception {
        IResourceExtractor extractor = new TreeResourceExtractor();
        try (IResourceReader reader = new ResourceReaderFromKGStoreV1Service("http://194.225.227.161:8091/")) {
            extractor.setup(reader, 1000000);
        }
        return extractor;
    }


    /**
     * Tokenizes the string using delimiter and return the last part.
     *
     * @param typeIri
     * @param separator
     * @return
     */
    private static String clean(String typeIri, String separator) {
        if (typeIri.contains("#"))
            return typeIri.substring(typeIri.lastIndexOf(separator) + 1);
        return typeIri;
    }

    /**
     * returns value, or "null" if null
     *
     * @param input
     */
    private static String noD(Object input) {
        if (input == null)
            return "null";
        return input.toString();
    }

}