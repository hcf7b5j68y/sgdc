package sgdc.dc.algorithms.sgdc;

import sgdc.dc.coverage.CoverageProvider;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.evidenceset.build.sampling.RandomSampling;
import sgdc.dc.evidenceset.build.sampling.FocusedSampling;
import sgdc.dc.input.Input;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.util.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class SgdcRunner {

    protected int sampleRounds = 20;
    protected double efficiencyThreshold = 0.005d;

    static public AtomicLong sampleCount = new AtomicLong(0);

    public static String dataset;

    public static CoverageProvider coverageProvider ;
    public SgdcRunner(String dataset) {
        SgdcRunner.dataset = dataset;
    }

    public DenialConstraintSet run(Input input, PredicateBuilder predicates) throws InterruptedException, ExecutionException, IOException {
        SgdcRunner.coverageProvider = new CoverageProvider(input, predicates, 10000);
        long preStartTime = System.currentTimeMillis();
        log.info("Random Sampling");
        IEvidenceSet sampleEvidenceSet = new RandomSampling(predicates)
                .buildEvidenceSet(input);

        HashEvidenceSet set = new HashEvidenceSet();

        sampleEvidenceSet.getSetOfPredicateSets().forEach(set::add);
        log.info("Random Sampling time: {}ms" ,(System.currentTimeMillis()- preStartTime));
        log.info("Focused Sampling");
        long tmp = System.currentTimeMillis();
        double gamma = Double.parseDouble(System.getProperty("fs.gamma", "0.5"));
        long amin = Long.getLong("fs.amin", 1_000_000L);
        long amax = Long.getLong("fs.amax", 20_000_000L);
        long n = input.getLineCount();
        long m = input.getColumns().length;
        long auto = (long) Math.ceil(gamma * (double) n * (double) m);
        long maxAttempts = Long.getLong("fs.maxAttempts", -1L);
        if (maxAttempts <= 0) {
            maxAttempts = Math.max(amin, Math.min(amax, auto));
        }
        IEvidenceSet fullEvidenceSet = new FocusedSampling(predicates)
                .buildEvidenceSetWithBudget(set, input, Parameters.efficiencyThreshold,maxAttempts);
        log.info("Focused Sampling time: {}ms", (System.currentTimeMillis()- tmp));

        log.info("SGDC discovery");
        long sbleiStart = System.currentTimeMillis();

        Verification verifier = new Verification(input, predicates);

        SGDC sgdc =
                new SGDC(predicates, coverageProvider, verifier);
        int K = 300;
        DenialConstraintSet dcs = sgdc.discoverDCs(sampleEvidenceSet, fullEvidenceSet, Parameters.tau, K);

        log.info("SGDC discovery time: {}ms", (System.currentTimeMillis() - sbleiStart));
        dcs.minimize();
        return dcs;
    }

    private static Logger log = LoggerFactory.getLogger(SgdcRunner.class);

}


