package edu.mcw.rgd.expression.processor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.GeneExpressionDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.GeneExpression;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.expression.ExpressionObservationDocument;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.Study;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExpressionObservationProcessor {

    private static final Logger log = LogManager.getLogger(ExpressionObservationProcessor.class);

    private final GeneExpressionDAO geneExpressionDAO = new GeneExpressionDAO();
    private final OntologyXDAO ontologyXDAO = new OntologyXDAO();
    private final GeneDAO geneDAO = new GeneDAO();
    private final MapDAO mapDAO = new MapDAO();

    private String outputDirectory = ".";
    private int threadCount = 4;

    public static void main(String[] args) {
        ExpressionObservationProcessor processor = new ExpressionObservationProcessor();

        // Parse command line arguments
        String outputDir = ".";
        int threads = 4;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-o":
                case "--output":
                    if (i + 1 < args.length) {
                        outputDir = args[++i];
                    }
                    break;
                case "-t":
                case "--threads":
                    if (i + 1 < args.length) {
                        threads = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-h":
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
            }
        }

        processor.setOutputDirectory(outputDir);
        processor.setThreadCount(threads);

        try {
            processor.processAllStudies();
        } catch (Exception e) {
            log.error("Error processing studies: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: ExpressionObservationProcessor [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -o, --output <dir>    Output directory for JSON files (default: current directory)");
        System.out.println("  -t, --threads <num>   Number of parallel threads (default: 4)");
        System.out.println("  -h, --help            Show this help message");
        System.out.println();
        System.out.println("The processor will fetch all expression studies from the database and generate");
        System.out.println("a JSON file for each study in the specified output directory.");
        System.out.println("Output files are named: expression_observations_<studyId>.json");
    }

    /**
     * Process all studies from the database.
     *
     * @throws Exception if processing fails
     */
    public void processAllStudies() throws Exception {
        // Create output directory if it doesn't exist
        Path outputPath = Paths.get(outputDirectory);
        if (!Files.exists(outputPath)) {
            Files.createDirectories(outputPath);
            log.info("Created output directory: " + outputDirectory);
        }

        // Fetch all studies from database
        log.info("Fetching expression studies from database...");
        List<Study> studies = geneExpressionDAO.getGeneExpressionStudies();
        log.info("Found " + studies.size() + " studies to process");

        if (studies.isEmpty()) {
            log.warn("No studies found in database");
            return;
        }

        // Process studies in parallel
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicInteger processedCount = new AtomicInteger(0);

        for (Study study : studies) {
            executor.submit(() -> {
                try {
                    processStudy(study);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("Failed to process study " + study.getId() + ": " + e.getMessage(), e);
                    failCount.incrementAndGet();
                } finally {
                    int processed = processedCount.incrementAndGet();
                    if (processed % 10 == 0 || processed == studies.size()) {
                        log.info("Progress: " + processed + "/" + studies.size() + " studies processed");
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(24, TimeUnit.HOURS);

        log.info("Processing complete. Success: " + successCount.get() + ", Failed: " + failCount.get());
    }

    /**
     * Process a single study and generate JSON file.
     *
     * @param study The study to process
     * @throws Exception if processing fails
     */
    public void processStudy(Study study) throws Exception {
        int studyId = study.getId();
        String outputFile = Paths.get(outputDirectory, "expression_observations_" + studyId + ".json").toString();

        log.debug("Processing study " + studyId + " -> " + outputFile);

        List<GeneExpression> records = geneExpressionDAO.getExpressionMetaDataByStudyId(studyId);

        if (records.isEmpty()) {
            log.warn("No records found for study " + studyId);
            return;
        }

        Set<Integer> recordIds = records.stream()
                .map(r -> r.getGeneExpressionRecord().getId())
                .collect(Collectors.toSet());

        List<List<Integer>> batches = split(new ArrayList<>(recordIds), 500);
        processExpressionObservation(batches, outputFile);

        log.info("Generated: " + outputFile + " (study " + studyId + ", " + records.size() + " records)");
    }

    /**
     * Process a study by ID and generate JSON file.
     *
     * @param studyId    The study ID
     * @param outputFile The output file path
     * @throws Exception if processing fails
     */
    public void processStudy(int studyId, String outputFile) throws Exception {
        List<GeneExpression> records = geneExpressionDAO.getExpressionMetaDataByStudyId(studyId);
        Set<Integer> recordIds = records.stream()
                .map(r -> r.getGeneExpressionRecord().getId())
                .collect(Collectors.toSet());

        List<List<Integer>> batches = split(new ArrayList<>(recordIds), 500);
        processExpressionObservation(batches, outputFile);
    }

    /**
     * Process expression observations from a list of record IDs.
     *
     * @param recordIds  Set of record IDs to process
     * @param outputFile The output file path
     * @throws Exception if processing fails
     */
    public void processExpressionObservation(Set<Integer> recordIds, String outputFile) throws Exception {
        List<List<Integer>> batches = split(new ArrayList<>(recordIds), 500);
        processExpressionObservation(batches, outputFile);
    }

    /**
     * Core method that processes batches and writes expression observation documents to JSON.
     *
     * @param batches    List of batched record IDs
     * @param outputFile The output file path
     */
    public void processExpressionObservation(List<List<Integer>> batches, String outputFile) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        File file = new File(outputFile);
        try (JsonGenerator generator = mapper.getFactory().createGenerator(file, JsonEncoding.UTF8)) {

            generator.writeStartArray();
            for (List<Integer> batch : batches) {
                List<GeneExpression> objects;
                try {
                    objects = geneExpressionDAO.getExpressionHighValuesByRecordIds(new HashSet<>(batch));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get expression values for batch", e);
                }

                for (GeneExpression expression : objects) {
                    try {
                        ExpressionObservationDocument document = createDocument(expression);
                        mapper.writeValue(generator, document);
                    } catch (Exception e) {
                        log.warn("Failed to process expression record: " + e.getMessage());
                    }
                }
            }
            generator.writeEndArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to write JSON file: " + outputFile, e);
        }
    }

    /**
     * Create an ExpressionObservationDocument from a GeneExpression record.
     *
     * @param expression The gene expression record
     * @return The document
     * @throws Exception if creation fails
     */
    private ExpressionObservationDocument createDocument(GeneExpression expression) throws Exception {
        ExpressionObservationDocument document = new ExpressionObservationDocument();

        // Build condition string
        List<Condition> conditions = geneExpressionDAO.getExpressionConditionsByRecordId(
                expression.getGeneExpressionRecord().getId());

        StringBuilder condition = new StringBuilder();
        condition.append(expression.getGeneExpressionRecord().getTraitTerm()).append(" | ");

        if(expression.getSample().getCellTypeAccId()!=null && !expression.getSample().getCellTypeAccId().equals("")) {
            Term cellType = ontologyXDAO.getTerm(expression.getSample().getCellTypeAccId());
            if (cellType != null && cellType.getTerm() != null && !cellType.getTerm().equals("")) {
                condition.append(cellType.getTerm()).append(" | ");
            }
        }
        condition.append(expression.getSample().getTissueTerm()).append(" | ");
        condition.append(expression.getSample().getStrainTerm()).append(" | ");
        condition.append(expression.getSample().getSex()).append(" | ");
        condition.append(expression.getSample().getLifeStage()).append(" | ");

        for (Condition c : conditions) {
            if (c.getOntologyId() != null && !c.getOntologyId().equals("")) {
                Term conditionTerm = ontologyXDAO.getTerm(c.getOntologyId());
                if (conditionTerm != null) {
                    condition.append(conditionTerm.getTerm()).append(" | ");
                }
                condition.append(c.getApplicationMethod());
            }
        }

        document.setCondition(condition.toString());
        document.setSample(expression.getSample().getGeoSampleAcc());
        document.setGene(expression.getGeneExpressionRecordValue().getExpressedGeneSymbol());
        document.setGeneRgdId(expression.getGeneExpressionRecordValue().getExpressedObjectRgdId());

        // Get gene map data
        Gene gene = geneDAO.getGene(expression.getGeneExpressionRecordValue().getExpressedObjectRgdId());
        if (gene != null) {
            List<MapData> mapDataList = getMapData(gene.getRgdId());
            if (mapDataList != null && !mapDataList.isEmpty()) {
                MapData map = mapDataList.get(0);
                document.setGeneStart(map.getStartPos());
                document.setGeneStop(map.getStopPos());
                document.setChr(map.getChromosome());
            }
        }

        document.setLevel(expression.getGeneExpressionRecordValue().getExpressionLevel());
        document.setTpm(expression.getGeneExpressionRecordValue().getExpressionValue());

        return document;
    }

    /**
     * Get map data for a given RGD ID.
     *
     * @param rgdId The RGD ID
     * @return List of MapData objects
     * @throws Exception if retrieval fails
     */
    protected List<MapData> getMapData(int rgdId) throws Exception {
        return mapDAO.getMapDataByRank(rgdId);
    }

    /**
     * Split a list into batches of specified size.
     *
     * @param list      The list to split
     * @param batchSize The batch size
     * @return List of batches
     */
    public static List<List<Integer>> split(List<Integer> list, int batchSize) {
        List<List<Integer>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            batches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return batches;
    }

    // Getters and Setters

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
