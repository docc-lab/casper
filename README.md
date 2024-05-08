# Systemizing and mitigating topological inconsistencies in Alibaba’s microservice call-graph datasets

## Overview
Alibaba released datasets of request workflows (CallGraphs) observed in their microservice architecture in 2021 and 2022. The dataset consists of many .csv.gz files. Rows of files describe caller/callee relationships between services. A trace ID links all rows belonging to the same trace together. So, request-workflow traces (CallGraphs) can be constructed by joining together rows with the same trace ID as per Alibaba's dataset format. More information about the Alibaba datasets, including their format, can be found here: ([2021](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021), [2022](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022).  Note that a single request's data may be split among multiple .csv.gz files.  

In this work, we identify inconsistencies in the dataset that result in inaccurately constructed traces.  We identify the inconsistencies in our ICPE'24 paper. This repository contains the code for CASPER, (`trace_builder.py`), a trace construction technique that use redundancies in the Alibaba dataset to mitigate inconsistencies. The CASPER source code also includes support for other trace construction methods ['modes'] for comparison purposes. 

Our paper introduces four rebuild modes that CASPER support: `naive-rpcid`, `naive-accurate`, `partial`, and `rebuild`. The CASPER algorithm is implemented under the `rebuild` mode. See the paper for a detailed explanation and comparison of these modes. 

We next describe the contents of this repository. They include 1) the CASPER source code (`trace_builder.py` and `process_traces.py`), 2) example input files to CASPER (2021/2022-shuffled-sample-traces-small/large.csv.gz) 3) the output of CASPER that we use in our paper (*.tar.gz), and 4) a python notebook with the experiments we ran on the data (`experiments.ipynb`). Input files to CASPER must be shuffled such that rows belonging to the same trace id are in a single input file.

## Running CASPER
Along with our code for CASPER, we released pre-shuffled trace files (two for 2021 and 2022). To run CASPER, you need at least one trace file, `trace_builder.py`, and `process_traces.py`.

### Requirements:
Must have pandas and numpy installed. 

### Usage:
CASPER takes as input a list of modes, with names as defined above. It supports two different output modes:
1. [`csv`] which outputs a tabular version of the traces in the same format as Alibaba's CallGraphs. This is the recommended output.
2. [`json`] which builds openTelemetry compatible traces (e.g. that can be imported into Jaeger). 

Set the rebuild mode(s) and output mode(s) in `process_traces.py`.

To run `casper`, use the command: `python3 process_traces.py <list of input file names>` (e.g. `python3 process_traces.py 2021-shuffled-sample-traces.csv.gz`)

### Notes:
1. The 2021 and 2022 datasets have slightly different formats. `casper` can only be run on data from one year at a time. There is a flag (default: 2021) in `process_traces.py` that must be set to the year of your input data to handle the different formats.
2. `casper` takes as input a list of shuffled trace files (`csv.gz` format).
3. There are many knobs you can adjust in `casper` (stored in `trace_builder.py`) including the number of traces per each output file and print verbosity.
4. `casper` requires multiple graph traversals so is slow, especially on the 2021 traces (since they have more errors than 2022). This is ok since `casper` is not meant to run repeatedly. Once you have the processed traces, those traces are meant to be used in the future.
   

## CASPER's output files (Output of the rebuild modes) 

We have released the output of CASPER that we used in the paper, corresponding to constructing traces as per the above modes.  The traces are stored in files titled `year-mode.tar.gz`. Each tarball contains a directory with processed trace files (`csv.gz`).  An additional subdirectory, `error-stats/`, is present if the mode is `rebuild').  

### Reconstructed traces:
In each tarball, the reconstructed traces are stored as `.csv.gz` files. Each file contains one or more traces. No trace spans multiple files. The format of a decompressed CSV file is the same as in the original datasets. 

### Statistics describing inconsistencies:
In the CASPER tarball(s), the statistics describing inconsistencies are stored in the `error-stats` folder. For each `.csv.gz`, there is a corresponding `global-stats` JSON file that records the total number of traces processed, the number of traces that are unaffected by inconsistencies, and the number of traces affected by each inconsistency.

Additionally, for [`rebuild`] mode, each `.csv.gz` has a corresponding `errors` JSON file. For each trace, it records metadata about the trace, the number of occurrences of different inconsistencies, and the details of each context propogation error. 

We collect the following data for each trace:
1. `total_num_rpcids`: total number of unique rpcids identified in the input
   file. 
2. `total_num_rows`: total number of rows for the trace id in the input file. 
3. `missing_rpcids`: total number of unique rpcids that are identified as
   missing (leaving a hole in the trace). This is just the fixable missing
   rpcids. 
4. `missing_duplicate_rows`: total number of duplicate rows that are missing.
   Rpctypes http and rpcid are expected to have exactly two rows. Only counts
   missing rows up until a CP error, but not downstream from it.  
5. `multi_um_rpcids`: total number of rpcids that have multiple unique UMs. We
   expect each rpcid to have a unique UM and DM, as it refers to a single call
   made in the system. We drop rows where the UM does not connect upstream (i.e.
   has conflicting information).
6. `unrecoverable_rpcids`: total number of unique rpcids that are deemed
   unrecoverable. This means the rpcid is downstream from a context propagation
   error, and cannot be uniquely connected to the trace. This also includes all
   rpcids affected by data loss below a context propagation error.
7. `unrecoverable_rows_dl`: Total number of rows that are unrecoverable (downstream
   from CP errors), affected by DL (dangling subtrees)
8. `unrecoverable_rows_path`: Total number of rows that are unrecoverable (downstream
   from CP errors), affected by non-unique paths 
9. `missing_um`: Total number of rows with NA or (?) in the UM cell. 
10. `missing_dm`: Total number of rows with NA or (?) in the DM cell. 
11. `missing_interface`: Total number of rows with NA or (?) in the interface cell. 

All of this information is stored under the key `METADATA` for the `tid` in
the errors_i.json files located in `error-stats/` in the output directory. Along
with the `METADATA` key, we have a key for each context propagation error in the
trace where the key is the rpcid and the value is an object with `num_ums`,
`num_dms`, `num_rows`, and `total_rows_impacted` (total number of downstream rows).

## Experiments 
The experiment script, `experiments.ipynb`, is used to generate all experiments in the paper.
