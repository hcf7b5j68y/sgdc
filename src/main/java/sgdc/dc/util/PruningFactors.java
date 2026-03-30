package sgdc.dc.util;

public class PruningFactors{
        public double a;
        public double b;
        public double threshold;
        public PruningFactors(double a, double b, double threshold){
            this.a = a;
            this.b = b;
            this.threshold = threshold;
        }

        public PruningFactors(){
            this.a = Parameters.a;
            this.b = Parameters.b;
            this.threshold = Parameters.tau;
        }
    }