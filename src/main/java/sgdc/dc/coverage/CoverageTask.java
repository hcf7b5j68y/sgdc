package sgdc.dc.coverage;

import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.evidenceset.build.sampling.RandomSampling;
import sgdc.dc.input.Input;
import sgdc.dc.predicates.PredicateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoverageTask {
    static public long time = 0;
    Input input;
    public String dataset;
    PredicateBuilder predicates;
    IEvidenceSet sampleEvidenceSet;
    CoverageHandler coverageHandler;
    long sampleSize;
    Map<DenialConstraint, Double> globalCoverageMap = new HashMap<>();


    public CoverageTask(String dataset,Input input, PredicateBuilder predicates, int sampleSize) {
        this.dataset = dataset;
        this.input = input;
        this.predicates = predicates;
        this.sampleSize = sampleSize;
        sampleEvidenceSet = new RandomSampling(predicates)
                .buildEvidenceSet(input);
        coverageHandler = new CoverageHandler(input, 1, sampleSize);
    }

    public void run(DenialConstraint dc, Map<DenialConstraint, Double> map, boolean isParallel) {

        coverageHandler.resetInvalidatedPairs();
        HashEvidenceSet set = new HashEvidenceSet();
        DenialConstraintSet generatedDCs = dc.generateSubset();
        sampleEvidenceSet.getSetOfPredicateSets().forEach(set::add);

        //CoverageSampler coverageSampler = new CoverageSampler(input, 1, sampleSize);

        int m = dc.size();
        double calculate = 0;
        List<DenialConstraint> list = generatedDCs.getConstraints().stream()
                .sorted(Comparator.comparing(DenialConstraint::getPredicateCount).reversed()).toList();


        for(DenialConstraint dc1:list){
            double v = coverageHandler.estimateCoverageParallel(dc1, m);
            //System.out.println(dc1+": "+ v);
            calculate += v;
        }
        calculate += (double) coverageHandler.getRemainingPairsCount() /sampleSize/m;
        map.put(dc, calculate);
    }


    private static Logger log = LoggerFactory.getLogger(CoverageTask.class);
}


