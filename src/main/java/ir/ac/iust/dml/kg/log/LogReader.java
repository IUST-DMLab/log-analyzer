package ir.ac.iust.dml.kg.log;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import ir.ac.iust.dml.kg.entity.extractor.Entity;
import ir.ac.iust.dml.kg.entity.extractor.IEntityExtractor;
import ir.ac.iust.dml.kg.entity.extractor.tree.TreeEntityExtractor;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by ali on 17/02/17.
 */
public class LogReader {
    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Usage: java -jar LogAnalyzer.jar path-to-log-name path-to-entity-redirects-json-file");
            System.exit(0);
        }
        String fileName = args[0];
        IEntityExtractor extractor = new TreeEntityExtractor();
        extractor.setup(args[1],args[2]);

        Map<String,Long> queriesFreq = new HashMap<>();

        Files.lines(Paths.get(fileName))
                .parallel()
                .skip(1)
                .map(l -> LogRecord.ParseLine(l))
                .filter(lr -> lr != null)
                .forEach(lr -> queriesFreq.put(lr.getQuery(), lr.getFreq()+queriesFreq.getOrDefault(lr.getQuery(),0l)));

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("analysis_results.txt"))) {

            Iterator<Map.Entry<String, Long>> itr = queriesFreq.entrySet()
                    .stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()).iterator();

            while(itr.hasNext()){
                Map.Entry<String, Long> e = itr.next();
                writer.write(String.format("Query: \"%s\" x %d times\n", e.getKey(), e.getValue()));

                List<Entity> detectedEntities = extractor.search(e.getKey(),true);
                for (Entity detectedEntity : detectedEntities)
                    writer.write(String.format("\tEntity: %s\n", detectedEntity.getEntity()));

                writer.write("\n");
            }
        }
    }
}