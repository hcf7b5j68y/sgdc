import sgdc.dc.algorithms.sgdc.SgdcRunner;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.input.Input;
import sgdc.dc.input.RelationalInput;

import sgdc.dc.predicates.PredicateBuilder;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.InputIterationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;


public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws AlgorithmExecutionException, ExecutionException, InterruptedException, IOException {
        double minimumSharedValue = 0.15d;
        Boolean noCrossColumn = Boolean.TRUE;

        String name = System.getProperty("dataset");

        if(name == null || name.isEmpty()){
            name = "";//input here
        }

        System.out.println("dataset: "+name);
        File file = new File(name);
        RelationalInput relationalInput = new RelationalInput(file);
        Input input = new Input(relationalInput);
        PredicateBuilder predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue);
        System.out.println("Predicate space size:" + predicates.getPredicates().size());
        long t01 = System.currentTimeMillis();

        DenialConstraintSet dcs = (new SgdcRunner(name)).run(input, predicates);
        log.info("total runtime: " + (System.currentTimeMillis()-t01)+"ms");
    }

}
