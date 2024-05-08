import sys
import trace_builder

# output directory. If it does not exist, trace_builder will create it and all
# needed sub-directories 
output_trace_dir = './output-2021/'

# settings: mode is a list of different trace construction modes
#           output-mode is output type, can be csv and or json
kwargs = {"mode": ['naive-accurate',
                   'naive-rpcid', 'partial', 'rebuild'], 'output-mode': ['csv']}

try: 
    builder = trace_builder.AlibabaTraceBuilder(**kwargs)

    # Get the list of input CSV files from command line arguments
    input_csv_files = sys.argv[1:]
    # used if running this in parallel (to name files uniquely)
    thread_ctr = 0
    file_ctr = 0
    # Set directories
    builder.set_year('2021') # alt: 2022

    builder.set_trace_files(input_csv_files)
    builder.set_thread_counter(thread_ctr)
    builder.init_file_counter(file_ctr)
    builder.set_output_dir(output_trace_dir)
    # Process files
    builder.build_traces()
except KeyboardInterrupt:
    print("Caught interrupt, exiting.")
    sys.exit(1)

