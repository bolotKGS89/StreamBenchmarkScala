# Compile and run FraudDetection

## Compile
sbt clean compile or sbt clean package

## Run
Example: java -cp target/FraudDetection-1.0.jar FraudDetection.FraudDetection --rate 0 --sampling 100 --parallelism 1 1 1

In the example above, we start the program with parallelism 1 for each operator (Source, Predictor and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).
