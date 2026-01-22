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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionObservationProcessor {

    private final GeneExpressionDAO geneExpressionDAO = new GeneExpressionDAO();
    private final OntologyXDAO ontologyXDAO = new OntologyXDAO();
    private final GeneDAO geneDAO = new GeneDAO();
    private final MapDAO mapDAO = new MapDAO();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ExpressionObservationProcessor <studyId> [outputFile]");
            System.out.println("  studyId    - The study ID to process");
            System.out.println("  outputFile - Optional output file name (default: expression_observations_<studyId>.json)");
            System.exit(1);
        }

        String studyId = args[0];
        String outputFile = args.length > 1 ? args[1] : "expression_observations_" + studyId + ".json";

        ExpressionObservationProcessor processor = new ExpressionObservationProcessor();
        try {
            processor.processStudy(studyId, outputFile);
            System.out.println("Successfully wrote expression observations to: " + outputFile);
        } catch (Exception e) {
            System.err.println("Error processing study: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Process a study and generate expression observation JSON file.
     *
     * @param studyId    The study ID (term_acc)
     * @param outputFile The output file path
     * @throws Exception if processing fails
     */
    public void processStudy(String studyId, String outputFile) throws Exception {
        List<GeneExpression> records = geneExpressionDAO.getExpressionMetaDataByStudyId(Integer.parseInt(studyId));
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
     * Extracted from ExpressionStudyDetails.setExpressionObservation()
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
                List<GeneExpression> objects = null;
                try {
                    objects = geneExpressionDAO.getExpressionHighValuesByRecordIds(new HashSet<>(batch));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (GeneExpression expression : objects) {
                    ExpressionObservationDocument document = new ExpressionObservationDocument();
                    List<Condition> conditions = null;
                    try {
                        conditions = geneExpressionDAO.getExpressionConditionsByRecordId(expression.getGeneExpressionRecord().getId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    StringBuilder condition = new StringBuilder();
                    condition.append(expression.getGeneExpressionRecord().getTraitTerm()).append(" | ");
                    Term cellType = null;
                    try {
                        cellType = ontologyXDAO.getTerm(expression.getSample().getCellTypeAccId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    condition.append(cellType.getTerm()).append(" | ");
                    condition.append(expression.getSample().getTissueTerm()).append(" | ");
                    condition.append(expression.getSample().getStrainTerm()).append(" | ");
                    condition.append(expression.getSample().getSex()).append(" | ");
                    condition.append(expression.getSample().getLifeStage()).append(" | ");
                    for (Condition c : conditions) {
                        try {
                            condition.append(ontologyXDAO.getTerm(c.getOntologyId()).getTerm()).append(" | ");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        condition.append(c.getApplicationMethod());
                    }
                    document.setCondition(condition.toString());
                    document.setSample(expression.getSample().getGeoSampleAcc());
                    document.setGene(expression.getGeneExpressionRecordValue().getExpressedGeneSymbol());
                    document.setGeneRgdId(expression.getGeneExpressionRecordValue().getExpressedObjectRgdId());
                    Gene gene = null;
                    try {
                        gene = geneDAO.getGene(expression.getGeneExpressionRecordValue().getExpressedObjectRgdId());
                        List<MapData> mapDataList = getMapData(gene.getRgdId());
                        if (mapDataList != null && !mapDataList.isEmpty()) {
                            MapData map = mapDataList.get(0);
                            document.setGeneStart(map.getStartPos());
                            document.setGeneStop(map.getStopPos());
                            document.setChr(map.getChromosome());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    document.setLevel(expression.getGeneExpressionRecordValue().getExpressionLevel());
                    document.setTpm(expression.getGeneExpressionRecordValue().getExpressionValue());
                    mapper.writeValue(generator, document);
                }
            }
            generator.writeEndArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
