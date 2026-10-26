"""Real single-node Big Data path: HDFS + YARN + Spark (batch, MLlib, Structured Streaming) + Kafka.

The pandas pipeline in ``hfs.features`` stays the reference implementation; every Spark transform here mirrors it
(same QC rules, units, PKT day boundaries, train-only climatology, labels, lags/rolling) so that results are comparable.
See results/bigdata/RUN_LOG.md and DEVIATIONS.md (D-BD*) for what was run and its caveats.
"""
