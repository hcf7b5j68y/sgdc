package sgdc.dc.coverage;

import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.input.Input;
import sgdc.dc.predicates.PredicateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoverageProvider {
    static public long time = 0;
    Input input;
    PredicateBuilder predicates;
    CoverageHandler coverageHandler;
    int sampleSize;
    private static final Map<DenialConstraint, Double> globalCoverageMap = new HashMap<>();


    public CoverageProvider(Input input, PredicateBuilder predicates, int sampleSize) {
        this.input = input;
        this.predicates = predicates;
        this.sampleSize = sampleSize;

        coverageHandler = new CoverageHandler(input, 1, sampleSize);
    }

    public double calculate(DenialConstraint dc) {

        if(globalCoverageMap.containsKey(dc))
            return globalCoverageMap.get(dc);

        coverageHandler.resetInvalidatedPairs();

        DenialConstraintSet generatedDCs = dc.generateSubset();


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
        globalCoverageMap.put(dc, calculate);
        return calculate;
    }


    private static Logger log = LoggerFactory.getLogger(CoverageProvider.class);
}
