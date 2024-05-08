# trace_builder is used to convert Alibaba's trace format to 
# opentelemetry (JSON) traces. I have 4 different building modes:
# naive-accurate, naive-rpcid, partial, and rebuild.
# It supports rebuilding traces that span many input files. 
# 
# It uses the following data structures:
#   - open_traces: dataframe storing all rows associated with trace_ids
#           that have not been processed/output yet
#   - current_trace_ids: list of trace_ids that are candidate for processing
#           when used, we check if all of the files containing the trace_id
#           have been loaded to open_traces before processing. 

# Note: assumes all rows for a trace_id are in the same input file. This is not
# true for the unprocessed traces alibaba released. We shuffled the data during
# preprocessing. 

# modes:
#   - naive-accurate: only keep traces that have no errors 
#                   (both inconsistencies in the data or topological errors)
#   - naive-row: treat each row as a unique call (or edge) in the trace
#   - naive-rpcid: treat each rpcid as a unique call (or edge) in the trace
#   - partial: only keep portions of the traces not impacted by an error. 
#               Discard rpcs that are downstream from any type of error.
#   - rebuild: correct errors, rebuilding the largest accurate traces as 
#               possible. We cannot rebuild downstream from context propagation
#               errors. rebuild mode is the same as CASPER. 

# output-modes:
#       - json: rebuild traces according to openTelemetry's tracing format. 
#               Outputs one JSON file per trace, named with the trace_id as 
#               the filename. [DEFAULT]
#       - csv: outputs a modified version of the tabular data used as input. 
#               This is still in alibaba's trace format. For naive-row: we 
#               leave all data. For naive-rpcid: we leave one row per rpcid. 
#               For naive-accurate: we leave all rows for traces that have no
#               errors. For partial: we omit rows that are downstream from any 
#               type of error: For rebuild: we modify rows that are fixed 
#               during error correction. 
import csv
import pandas as pd
import json
import os
import numpy as np
from sys import exit
import re
import math


class AlibabaTraceBuilder:
    def __init__(self, **kwargs):
        self.args = kwargs
        self.output_dir = None
        self.trace_files = None
        self.thread_ctr = -1

        # Must be set to the year the input data was released (based on
        # formatting differences)
        self.year = '2022'
        
        # set to True when you want to skip building traces if file already exists. 
        self.faster = True
        pd.options.mode.chained_assignment = None

        # dataframe with traces we are currently rebuilding
        self.open_traces = None

        # dataframe with processed trace data 
        self.processed_traces_row = pd.DataFrame()
        self.processed_traces_rpcid = pd.DataFrame()
        self.processed_traces_accurate = pd.DataFrame()
        self.processed_traces_partial = pd.DataFrame()
        self.processed_traces_rebuild = pd.DataFrame()
        self.current_output_file_id = 0

        self.output_traces_per_file = 20000
        
        # print status every print_verbosity traces
        self.print_verbosity = 150

        # FOR TESTING
        self.num_traces_currently_processed = 0
        
        self.current_trace_ids = None
        # global counters
        self.n_traces = 0
        self.n_unaffected_traces = 0
        self.n_cp_affected_traces = 0
        self.n_dl_affected_traces = 0

        self.trace_with_bad_um_rpcids = 0
        
        # per file counters
        self.n_traces_in_file = 0
        self.n_unaffected_traces_in_file = 0
        self.n_cp_affected_traces_in_file = 0
        self.n_dl_affected_traces_in_file = 0

        # for building traces
        # process data structure for traces
        self.processes = {}
        # process_map: DM --> pid
        self.p_map = {}

        # to collect error stats on traces
        self.trace_errors = None

        self.downstream_rows = None
    
    def set_output_dir(self, path):
        self.output_dir = path
        self.init_output_dirs()

    # if output dir dne, create it and all subdirs needed 
    def init_output_dirs(self):
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
        # create subdirectories for each mode's output data
        for mode in self.args['mode']:
            os.makedirs(self.modes_output_dir(mode), exist_ok=True)
        return

    def set_year(self, year):
        self.year = year

    def print_trace_stats(self):
        print('===========PER FILE INFO===============')
        print('num traces: ', self.n_traces_in_file)
        print('num traces with CP errors: ', self.n_cp_affected_traces_in_file)
        print('num traces with DL errors: ', self.n_dl_affected_traces_in_file)
        print('============GLOBAL INFO=================')
        print('num traces: ', self.n_traces)
        print('num traces with CP errors: ', self.n_cp_affected_traces)
        print('num traces with DL errors: ', self.n_dl_affected_traces)
        return

    def dump_data_to_file(self):
        for service in self.service_in_traces:
            self.service_in_traces[service] = (len(
                self.service_in_traces[service]) / self.n_traces_in_file) * 100
            
        with open(self.output_dir + 'service_in_traces.json', 'w') as outfile:
            outfile.write(json.dumps(self.service_in_traces))
        return
    
    def set_trace_files(self, files):
        self.trace_files = files
        return
    
    def set_thread_counter(self, ctr):
        self.thread_ctr = ctr
        return
    
    def init_file_counter(self, ctr):
        self.current_output_file_id = int(ctr)
        return

    def build_traces(self):
        trace_files = self.trace_files
        
        for index, tfname in enumerate(trace_files):
            print('processing file', index + 1, 'out of', len(trace_files), tfname)
            
            self._open_new_file_of_traces(tfname)

            # make missing vals (UNKNOWN) be (?) similar to 2021
            if self.year == '2022':
                self.open_traces['um'] = self.open_traces['um'].replace('UNKNOWN', '(?)')
                self.open_traces['dm'] = self.open_traces['dm'].replace('UNKNOWN', '(?)')

            # update list of open trace ids
            self.process_traces()
            
    # adding new traces to open traces and updating list of tids that 
    # can be processed
    def _open_new_file_of_traces(self, filename):
        new_traces = pd.read_csv(filename, compression='gzip', dtype=str)

        if self.year == '2022':
            new_traces.rename(columns={'rpc_id': 'rpcid'}, inplace=True)
            # NOTE: dropping traces with odd rows, this is rare.
            total_traceids = new_traces['traceid'].nunique()
            bad_traceids = new_traces[(new_traces['rt'] == 'None') | (
                new_traces['rt'].str.startswith('MS_', na=False))]['traceid'].unique().tolist()
            new_traces = new_traces[~new_traces['traceid'].isin(bad_traceids)]

            print('filtering # traces with crazy rows: ', len(bad_traceids),
                  ' (', len(bad_traceids) / total_traceids * 100, '%)')
            
            new_traces.drop(
                ['timestamp', 'service', 'uminstanceid', 'dminstanceid'], axis=1, inplace=True)
            
        new_traces['rt'] = new_traces['rt'].astype(float)

        self.open_traces = pd.concat([self.open_traces, new_traces], axis=0)
        self._update_current_trace_ids()
        
    def _close_trace(self, tid):
        self.open_traces = self.open_traces[self.open_traces['traceid'] != tid]
        self.current_trace_ids.remove(tid)
        # delete process info
        self.p_map = {}
        self.processes = {}

        self.processed_traces_row = pd.DataFrame()
        self.processed_traces_rpcid = pd.DataFrame()
        self.processed_traces_accurate = pd.DataFrame()
        self.processed_traces_partial = pd.DataFrame()
        self.processed_traces_rebuild = pd.DataFrame()
        
    def _update_current_trace_ids(self):
        self.current_trace_ids = self.open_traces['traceid'].unique().tolist()
        return
    
    # checks if filename with trace id exists in output
    # if so, don't process this trace (to reduce duplicate work)
    def trace_file_exists(self, tid):
        return os.path.isfile(self.output_dir + str(tid) + '.json')
        
    # called per each file, tried to process all complete traces seen so far 
    def process_traces(self):
        self.trace_errors = {} 
        self.n_traces_in_file = 0
        self.n_unaffected_traces_in_file = 0
        self.n_cp_affected_traces_in_file = 0
        self.n_dl_affected_traces_in_file = 0
        # key is trace_id, value is dict: key is rpcid with context prop error, value is # of rows impacted by error

        try:
        # for each trace
            while (len(self.current_trace_ids) > 0):
                tid = self.current_trace_ids[0]

                self.n_traces_in_file += 1
                # print every status every print_verbosity traces
                if self.n_traces_in_file % self.print_verbosity == 0:
                    self.print_stats()

                cp_affected = False
                dl_affected = False

                # NOTE: this way of storing is inefficient, think about hashing.
                trace = self.open_traces[self.open_traces['traceid'] == tid]
                    
                # get list of rpcids in this trace
                all_rpcids = trace['rpcid'].unique().tolist()
                rpcids = self.sort_rpcids(all_rpcids)

                # get root rpcids
                root_rpcids = self.find_all_roots(rpcids)

                #initilize errors for this trace
                self.init_error_info(
                    tid, trace.shape[0], len(rpcids), len(root_rpcids))
                
                self.preprocess_missing_values(trace)

                # to terminate trace reconstruction for partial and accurate
                # modes
                trace_has_error = False
                partial_rpcids = rpcids.copy()
                in_partial = True

                # for each rpcid: breadth first traversal of trace
                while(len(rpcids) > 0):
                    rpcid = rpcids.pop(0)
                    # to maintain partial rpcid mode:
                    if rpcid not in partial_rpcids:
                        in_partial = False
                    else:
                        in_partial = True
                        partial_rpcids.remove(rpcid)

                    # extract rows for this rpcid
                    rpcs = trace[trace['rpcid'] == rpcid]

                    # mode: rebuild without error checking
                    if ('naive-rpcid' in self.args['mode']):
                        # only save one row per rpcid in self.processed_traces
                        self.processed_traces_rpcid = self.add_rows_to_processed_traces(
                            rpcs.head(1), self.processed_traces_rpcid)
                    if ('naive-row' in self.args['mode']):
                        self.processed_traces_row = self.add_rows_to_processed_traces(
                            rpcs, self.processed_traces_row)
                    # check for errors, handle appropriately for mode
                    if ('naive-accurate' in self.args['mode'] or 'partial' in self.args['mode'] or 'rebuild' in self.args['mode']):
                        error_here = False
                        dl_rebuild = False
                        cp_rebuild = False
                        # remove rows that don't connect to parent rpc's DM
                        original_ums = rpcs['um'].unique().tolist()
                        rpcs = self.filter_non_connecting_rows(rpcid, trace, rpcs)

                        # NOTE: should do this before filtering bad UMS 
                        # and chain to UM/DM pairs
                        # self.calculate_missing_duplicate_rows(rpcs)

                        # deal with downstream missing values
                        rpcs = self.fix_missing_dms(trace, rpcs)

                        # for detecting CP errors.
                        num_calls = self.get_num_unique_calls(rpcs)

                        # if there's data loss from this rpcid 
                        if (self.check_for_data_loss(tid, rpcid, root_rpcids, all_rpcids)):
                            error_here = True
                            dl_affected = True
                            # mode: delete traces with inaccuracies
                            if ('naive-accurate' in self.args['mode'] and not trace_has_error):
                                self.processed_traces_accurate = self.drop_trace_from_processed_traces(
                                    tid, self.processed_traces_accurate)
                                trace_has_error = True
                            if ('partial' in self.args['mode']):
                                # don't connect broken parts of trace
                                partial_rpcids = self.delete_downstream_rpcids(
                                    partial_rpcids, trace, rpcid)
                                # mark current node as not being in partial trace
                                in_partial = False 
                            if ('rebuild' in self.args['mode']):
                                dl_rebuild = True
                        # if context prop error rows. 
                        if num_calls > 1:
                            error_here = True
                            cp_affected = True

                            # save information about this context prop error
                            self.store_cp_error_stats(
                                tid, rpcid, trace, len(original_ums), rpcs['dm'].nunique(), rpcs.shape[0])

                            # mode: delete traces with errors
                            if ('naive-accurate' in self.args['mode'] and not trace_has_error):
                                self.processed_traces_accurate = self.drop_trace_from_processed_traces(tid, self.processed_traces_accurate)
                                trace_has_error = True
                            # mode: partial traces
                            if ('partial' in self.args['mode']):
                                # delete this rpcid and all downstream rpcids, since they are not accurate. 
                                partial_rpcids = self.delete_downstream_rpcids(
                                    partial_rpcids, trace, rpcid)
                                in_partial = False
                            # mode: fix error
                            if ('rebuild' in self.args['mode']):
                                cp_rebuild = True

                                # get downstream rpcids in trace
                                downstream_rpcids = self.get_downstream_rpcids(rpcids, rpcid)
                                
                                # extract downstream rows 
                                downstream_rows = trace[trace['rpcid'].isin(
                                    downstream_rpcids)]
                                
                                # fix this rpcid and downstream, when possible
                                self.connect_downstream_from_cp(
                                    rpcid, downstream_rpcids, downstream_rows)
                                
                                # remove rpcids from current processing
                                rpcids = self.delete_downstream_rpcids(rpcids, trace, rpcid)
                        # if we fixed the dl but didn't add this row for
                        # rebuild, add it now.
                        # no dataloss, build span.
                        if (not error_here or (dl_rebuild and not cp_rebuild)):
                            try:
                                self.add_row_for_modes(
                                    rpcs.head(1), trace_has_error, in_partial)
                            except:
                                print('cannot build span!', str(self.thread_ctr))

                self.update_global_error_counters(cp_affected, dl_affected)

                self.num_traces_currently_processed += 1

                # output data for the currently processed trace
                self.output_trace_data()

                # clean up data structures to remove trace
                self._close_trace(tid)

                # if we're processed traces per file: 
                if self.num_traces_currently_processed + 1 == self.output_traces_per_file:
                    self.output_error_stats()
                    # clear errors
                    self.trace_errors = {}
        except KeyboardInterrupt:
            print('Saving data to resume rebuilding.')
            
        print('# traces processed in this file:', len(self.trace_errors.keys()))
        print('cp_affected_traces: ', self.n_cp_affected_traces_in_file)
        print('dl_affected_traces: ', self.n_dl_affected_traces_in_file)
        print('unaffected_traces', self.n_unaffected_traces_in_file)

        #  delete current processed traces.
        self.processed_traces = pd.DataFrame()
        # Update global stats
        self.n_traces += self.n_traces_in_file
        
        self.n_unaffected_traces += self.n_unaffected_traces_in_file
        self.n_cp_affected_traces += self.n_cp_affected_traces_in_file
        self.n_dl_affected_traces += self.n_dl_affected_traces_in_file
        self.print_stats(True)
        self.output_error_stats()

    def add_row_for_modes(self, row, trace_has_error, in_partial):
        # only add row if we haven't seen an error yet
        if ('naive-accurate' in self.args['mode'] and not trace_has_error):
            self.processed_traces_accurate = self.add_rows_to_processed_traces(
                row, self.processed_traces_accurate)
        if ('partial' in self.args['mode'] and in_partial):
            self.processed_traces_partial = self.add_rows_to_processed_traces(
            row, self.processed_traces_partial)
        if ('rebuild' in self.args['mode']):
            self.processed_traces_rebuild = self.add_rows_to_processed_traces(
            row, self.processed_traces_rebuild)

        return


    def output_trace_data(self):
        # close this csv, and open a new one with i+1
        if self.num_traces_currently_processed == self.output_traces_per_file:
            self.current_output_file_id += 1
            self.num_traces_currently_processed = 0
        
        # check each mode and output mode
        if ('csv' in self.args['output-mode']):
            # print('------outputting csv files-------')
            if ('naive-row' in self.args['mode']):
                self.output_csv_traces(self.processed_traces_row, 'naive-row')
            if ('naive-rpcid' in self.args['mode']):
                self.output_csv_traces(self.processed_traces_rpcid, 'naive-rpcid')
            if ('naive-accurate' in self.args['mode']):
                self.output_csv_traces(self.processed_traces_accurate, 'naive-accurate')
            if ('partial' in self.args['mode']):
                self.output_csv_traces(self.processed_traces_partial, 'partial')
            if ('rebuild' in self.args['mode']):
                self.output_csv_traces(self.processed_traces_rebuild, 'rebuild')

        if ('json' in self.args['output-mode']):
            # print('------outputting processed traces-------')
            if ('naive-row' in self.args['mode']):
                self.output_span_traces(self.processed_traces_row, 'naive-row')
            if ('naive-rpcid' in self.args['mode']):
                self.output_span_traces(
                    self.processed_traces_rpcid, 'naive-rpcid')
            if ('naive-accurate' in self.args['mode']):
                self.output_span_traces(
                    self.processed_traces_accurate, 'naive-accurate')
            if ('partial' in self.args['mode']):
                self.output_span_traces(
                    self.processed_traces_partial, 'partial')
            if ('rebuild' in self.args['mode']):
                self.output_span_traces(
                    self.processed_traces_rebuild, 'rebuild')
        return 
    
    def print_stats(self, all=False):
        if all:
            print('process: ', str(self.thread_ctr), 'Global stats! total:', self.n_traces, ' unaffected: ', self.n_unaffected_traces /
                  self.n_traces * 100, '% \n CP: ', self.n_cp_affected_traces / self.n_traces * 100, '% \n DL: ', self.n_dl_affected_traces / self.n_traces * 100)
        else:
            print('process: ', str(self.thread_ctr), 'Processed', self.n_traces_in_file, '. Unaffected: ', self.n_unaffected_traces_in_file /
                  self.n_traces_in_file * 100, '%. \n Bad UM traces: ', self.trace_with_bad_um_rpcids / self.n_traces_in_file * 100, ' CP errors: ', self.n_cp_affected_traces_in_file / self.n_traces_in_file * 100, ' DL traces: ', self.n_dl_affected_traces_in_file / self.n_traces_in_file * 100)
        return
    
    # if output mode is json, create trace objects and output
    def output_span_traces(self, processed_traces, mode):
        if processed_traces.empty:
            return
            
        # get processed tid
        tid = processed_traces['traceid'].unique().tolist()[0]
        self.build_trace_file(
            tid, processed_traces, mode)
        return
    
    # Always checks if UM span doesn't exist, if so creates it
    # Note: this does not work for mode naive-rows since there is 
    # no way to connect the edges given repeated rpcids. 
    def build_trace_file(self, tid, rows, mode):
        # keep trace of current nodes, add UM when it doesn't
        # exist yet 
        current_spans = set()

        self.start_trace_file(tid, mode)
        first_span = True
        # get rows in dataframe for traces
        rpcids = self.sort_rpcids(rows['rpcid'].unique().tolist())

        # add edge for each row 
        for rpcid in rpcids:
            # get row(s) for the current rpcid
            rpc_rows = rows[rows['rpcid'] == rpcid]
            # add edge for each row

            # check if UM span does not exist  
            um_span_id = rpcid.rsplit('.', 1)[0]
            if um_span_id not in current_spans:
                name = rpc_rows['um'].unique().tolist()[0]
                interface = rpc_rows['interface'].unique().tolist()[0]
                first_span = self.build_span(tid, rpcid, name, interface,
                                mode, True, first_span)
                current_spans.add(um_span_id)
            
            name = rpc_rows['dm'].unique().tolist()[0]
            interface = rpc_rows['interface'].unique().tolist()[0]
            self.build_span(tid, rpcid, name, interface,
                            mode, False)
            current_spans.add(rpcid)

        self.end_trace_file(tid, mode)
        return
    
    # output current processed traces in csv format. 
    def output_csv_traces(self, processed_traces, mode):
        if processed_traces.empty:
            # print('no processed traces to output')
            return
        # write processed trace to csv)
        with open(self.modes_output_dir(mode) + str(self.thread_ctr) + '-' + mode + '-' + str(self.current_output_file_id) + '.csv', 'a') as f:
            processed_traces.to_csv(f, mode='a', header=f.tell() == 0)
        return 
    
    # downstream_rpcids should be sorted when passed in
    # only for impacted rpcids (downstream from a CPE)
    # Takes all rpcids downstream from a CPE and looks for unique call paths to
    # connect to the trace. 
    # Has two phases: 1) delete dangling trees that are affected by data loss
    #  and 2) verify forms a unique call paths. The final step is to update the
    #  rpcids to be unique 
    def connect_downstream_from_cp(self, cp_rpcid, downstream_rpcids, downstream_rows):
        tid = downstream_rows['traceid'].iloc[0]
        downstream_r = downstream_rpcids.copy()
        self.downstream_rows = downstream_rows

        # 1. delete dangling downstream trees (anything affected by DL)
        indices_to_drop = []
        while len(downstream_r) > 0:
            rpcid = downstream_r.pop(0)
            # get children 
            children = self.find_direct_descendants(rpcid, downstream_rpcids)
            # if there's no direct child, we found a hole or leaf
            if len(children) == 0:
                # get descendants 
                descendants = self.find_all_descendants(rpcid, downstream_rpcids)
                self.trace_errors[tid]['METADATA']['unrecoverable_rpcids'] += len(descendants)
                
                # delete all descendants 
                downstream_rpcids = [
                    r for r in downstream_rpcids if r not in descendants]
                downstream_r = [
                    r for r in downstream_r if r not in descendants]
                
                index_to_delete = downstream_rows[downstream_rows['rpcid'].isin(
                    descendants)].index.tolist()
                if len(index_to_delete) > 0:
                    indices_to_drop.extend(index_to_delete)

        # drop rows associated to data loss
        self.trace_errors[tid]['METADATA']['unrecoverable_rows_dl'] = len(
            indices_to_drop)
        if len(indices_to_drop) > 0:
            self.downstream_rows.drop(indices_to_drop, inplace=True)

        # 2. Path validation (only keep rows that can be connected uniquely)
        # for each row:
        for i, row in downstream_rows.iterrows():
            parent_rpcid = row['rpcid'].rsplit('.', 1)[0]

            # if we're on root of cp error, skip since already done
            if parent_rpcid not in downstream_rpcids or '.' not in row['rpcid']:
                continue

            rows_for_parent = self.downstream_rows[self.downstream_rows['rpcid']
                                              == parent_rpcid]
            # if parent_rpcid exists (this should always be true bc no DL).
            if rows_for_parent.shape[0] > 0:
                # if parent rpcid has multiple DM calls that match
                parent_connecting_rows = rows_for_parent[rows_for_parent['dm'] == row['um']]
                num_connecting_calls = self.get_num_unique_calls(parent_connecting_rows)
                if num_connecting_calls != 1:
                    # mark this row as to remove
                    if i in self.downstream_rows.index:
                        self.downstream_rows.drop(i, inplace=True)

                    # get downstream info from this rpicd
                    downstream_rpcids_from_here = self.get_downstream_rpcids(downstream_rpcids, row['rpcid'])

                    downstream_rpcids_from_here.remove(row['rpcid'])

                    downstream_rows_from_here = self.downstream_rows[self.downstream_rows['rpcid'].isin(downstream_rpcids_from_here)]

                    self.delete_downstream_non_unique_from_DM(
                        downstream_rows_from_here, row, downstream_rpcids_from_here)
                               
        # update rpcids 
        self.trace_errors[self.downstream_rows['traceid'].iloc[0]]['METADATA']['unrecoverable_rpcids'] += len(downstream_rpcids) - self.downstream_rows['rpcid'].nunique()

        self.update_rpcids(cp_rpcid, None, downstream_rows, None)
        return
    
    # DFS to update rpcids to be unique
    # assumes all of the rpcids connect to the existing trace in a unique way
    # and are not affected by data loss. 
    def update_rpcids(self, this_rpcid, connecting_um, rows, corrected_parent=None): 
        if not corrected_parent:
            corrected_parent = this_rpcid.rsplit('.', 1)[0]

        if connecting_um:
            connecting_rows = rows[(
                (rows['um'] == connecting_um) & (rows['rpcid'] == this_rpcid))]
        else:
            connecting_rows = rows[rows['rpcid'] == this_rpcid]

        if connecting_rows.shape[0] == 0:
            return

        num_calls = self.get_num_unique_calls(connecting_rows)
        new_rpcid = this_rpcid.replace(
            this_rpcid.rsplit('.', 1)[0], corrected_parent, 1)
        if num_calls > 1:
            dms = connecting_rows['dm'].unique().tolist()
            for dm in dms:
                rows_to_dm = connecting_rows[connecting_rows['dm'] == dm]
                num_calls_dm = self.get_num_unique_calls(rows_to_dm)
                for n in range(num_calls_dm):
                    updated_new_rpcid = new_rpcid + '-' + str(dm) + '-' + str(n)
                    # output row with update rpcid
                    rows_to_dm['rpcid'] = updated_new_rpcid
                    self.processed_traces_rebuild = self.add_rows_to_processed_traces(
                        rows_to_dm.head(1), self.processed_traces_rebuild)
                # recurse to children
                if num_calls_dm == 1:
                    children = self.get_children_rpcids(rows['rpcid'].unique().tolist(), this_rpcid)
                    for child in children:
                        self.update_rpcids(
                            child, dm, rows, new_rpcid + '-' + str(dm) + '-0')
        else:
            # add this row
            connecting_rows['rpcid'] = new_rpcid
            self.processed_traces_rebuild = self.add_rows_to_processed_traces(
                connecting_rows.head(1), self.processed_traces_rebuild)
            
            # recurse
            children = self.get_children_rpcids(
                rows['rpcid'].unique().tolist(), this_rpcid)
            for child in children:
                self.update_rpcids(
                    child, connecting_rows['dm'].tolist()[0], rows, new_rpcid)

        return 
    
    # Assumes we fix missing DMs using duplicate records before this
    def get_num_unique_calls(self, rows):
        num_calls = 0
        dms = rows['dm'].unique().tolist()
        
        # special case for userDefined rpc with both + rt rows
        if len(dms) == 1 and rows.shape[0] == 2:
            rpctype = rows['rpctype'].unique().tolist()
            if len(rpctype) == 1 and 'userDefined' in rpctype:
                return 1
            
        for dm in dms:
            dm_rows = rows[rows['dm'] == dm]
            # group rows by rpctypes
            this_rpctypes = dm_rows['rpctype'].unique().tolist()
            for this_type in this_rpctypes:
                dm_rows_type = dm_rows[dm_rows['rpctype'] == this_type]
                num_calls += self.num_calls_year(dm_rows_type)

        return num_calls
    
    def expected_num_rows(self, type):
        if type in ['mc', 'mq', 'db']:
            return 1
        return 2

    # for 2021 traces 
    # Note: rows has the same UM, DM pair!
    def num_calls_year(self, rows):
        # check rpctype for expected number of rows
        expected_num_rows = self.expected_num_rows(rows['rpctype'].unique().tolist()[0])
        if expected_num_rows == 1:
            return rows.shape[0]
        
        if self.year == '2021':
            # pair all 0 rt rows first
            fast_calls = math.floor(rows[rows['rt'] == 0].shape[0] / 2)
            # if there's an extra 0 rt row, should pair it with a + row
            extra_fast_row = rows[rows['rt'] == 0].shape[0] % 2
            #  0 rt rows can only be matched with a positive, so they're
            #  treated as a negative row. 
            slower_calls = max(int(abs(rows[rows['rt'] < 0].shape[0] - extra_fast_row)), int(rows[rows['rt'] > 0].shape[0]))

            return slower_calls + fast_calls
        else: 
            return math.ceil(rows['rt'].shape[0] / 2)
    
    def delete_downstream_non_unique_from_DM(self, downstream_rows, bad_row, downstream_rpcids):
        cur_rpcid = bad_row['rpcid']
        drop_um = bad_row['dm']
        indices = self.filter_downstream_helper(
            cur_rpcid, downstream_rpcids, downstream_rows, drop_um)
        return indices

    # rpcid: parent/upstream rpcids that should be filtered from
    # downstream_rpcids: unique rpcids downstream from CPE
    # rows: downstream rows from rpcid
    # drop_um: dm of rpcid, anything you should filter that's one step below
    # rpcid and connects to it. 
    def filter_downstream_helper(self, rpcid, downstream_rpcids, rows, drop_um):
        # get next level of rpcids that connect to this one 
        children = self.find_direct_descendants(rpcid, downstream_rpcids)

        # filter children rows that connect to the UM to drop
        connecting_children_rows = self.downstream_rows[(self.downstream_rows['rpcid'].isin(children)) & (self.downstream_rows['um'] == drop_um)]

        if connecting_children_rows.shape[0] > 0:
            # get indices of rows to filter 
            index_to_delete = connecting_children_rows.index.tolist()
            self.downstream_rows.drop(index_to_delete, inplace=True)

        # move to next rpcid
        # for each row that is being filtered:
        connecting_children_rpcids = connecting_children_rows['rpcid'].unique().tolist()

        for child_rpcid in connecting_children_rpcids:
            # get list of next services that we should filter from here 
            filter_ums = connecting_children_rows[connecting_children_rows['rpcid'] == child_rpcid]['dm'].unique().tolist()

            for um in filter_ums:
                self.filter_downstream_helper(child_rpcid, downstream_rpcids, self.downstream_rows, um)
        return 
    
    def find_direct_descendants(self, this_rpcid, all_rpcids):
        pattern = f"^{re.escape(this_rpcid)}\.\d+$"
        # Filter the rpcids using the regex pattern
        return [rpcid for rpcid in all_rpcids if re.match(pattern, rpcid)]

    def find_all_descendants(self, this_rpcid, all_rpcids):
        pattern = f"^{re.escape(this_rpcid)}(\.\d+)+"
        # Filter the rpcids using the regex pattern
        return [rpcid for rpcid in all_rpcids if re.match(pattern, rpcid)]

    def drop_trace_from_processed_traces(self, tid, processed_traces):
        if not processed_traces.empty:
            processed_traces = processed_traces[processed_traces['traceid'] != tid]
        return processed_traces
    
    def add_rows_to_processed_traces(self, rows, processed_traces):
        return pd.concat([processed_traces, rows], ignore_index=True)


    def delete_downstream_rpcids(self, rpcids, trace, rpcid):
        return [row for row in rpcids if row not in trace['rpcid'].loc[trace['rpcid'].str.startswith(rpcid, na=False)].unique().tolist()]
    
    def get_downstream_rpcids(self, rpcids, this_rpcid):
        prefix = this_rpcid + '.'
        downstream_rpcids = [
            rpcid for rpcid in rpcids if rpcid.startswith(prefix)]
        downstream_rpcids.append(this_rpcid)
        return self.sort_rpcids(downstream_rpcids)
    
    def delete_downstream_from_DMs(self, filter_DMs, downstream_rpcids, trace):
        # 
        return


    def find_all_roots(self, rpcids):
        # Initialize Trie
        rpcid_trie = RpcidTrie()

        # Insert rpcids into Trie
        for rpcid in rpcids:
            rpcid_trie.insert(rpcid)

        # Find root rpcids
        root_rpcids = rpcid_trie.find_roots()
        return root_rpcids
    
    # filter out rows for this rpcid that don't connect to upstream
    def filter_non_connecting_rows(self, rpcid, trace, rpcs):
        ums = rpcs['um'].unique().tolist()
        # if multiple UMs, filter out ones that don't match the parent's dm, verifying valid path in traces.
        if (len(ums) > 1):
            tid = trace['traceid'].tolist()[0]

            # update error information
            self.trace_errors[tid]['METADATA']['multi_um_rpcids'] += 1
            # if this is the first bad um for this trace, increment counter.
            if self.trace_errors[tid]['METADATA']['multi_um_rpcids'] == 1:
                self.trace_with_bad_um_rpcids += 1

            # get dm of parent
            parent_rpcid = rpcid.rsplit('.', 1)[0]
            parent_dm = trace[trace['rpcid'] == parent_rpcid]
            # if parent exists:
            if parent_dm.shape[0] > 0:
                parent_dm = parent_dm['dm'].unique().tolist()[0]
                # filter rows with um that connects to parent call only
                rpcs = rpcs[rpcs['um'].isin([parent_dm, '(?)'])]
                # overwrite any missing values with correct UM
                rpcs['um'] = parent_dm
        return rpcs
    
    def init_error_info(self, tid, size, num_rpcids, num_roots):
        self.trace_errors[tid] = {}
        self.trace_errors[tid]['METADATA'] = {}
        self.trace_errors[tid]['METADATA']['total_num_rows'] = size
        self.trace_errors[tid]['METADATA']['multi_um_rpcids'] = 0
        self.trace_errors[tid]['METADATA']['unrecoverable_rpcids'] = 0
        self.trace_errors[tid]['METADATA']['unrecoverable_rows_dl'] = 0
        self.trace_errors[tid]['METADATA']['unrecoverable_rows_path'] = 0
        self.trace_errors[tid]['METADATA']['missing_rpcids'] = 0
        self.trace_errors[tid]['METADATA']['total_num_rpcids'] = num_rpcids
        self.trace_errors[tid]['METADATA']['num_roots'] = num_roots
        self.trace_errors[tid]['METADATA']['missing_um'] = 0
        self.trace_errors[tid]['METADATA']['missing_dm'] = 0
        self.trace_errors[tid]['METADATA']['missing_interface'] = 0
        return
    
    def store_cp_error_stats(self, tid, rpcid, trace, num_ums, num_dms, num_rows):
        self.trace_errors[tid][rpcid] = {}
        self.trace_errors[tid][rpcid]['num_ums'] = num_ums
        self.trace_errors[tid][rpcid]['num_dms'] = num_dms
        self.trace_errors[tid][rpcid]['num_rows'] = num_rows
        # count all downstream calls impacted
        self.trace_errors[tid][rpcid]['total_rows_impacted'] = trace['rpcid'].loc[trace['rpcid'].str.startswith(rpcid + '.', na=False)].shape[0]
        return 

    def preprocess_missing_values(self, trace):
        tid = trace['traceid'].iloc[0]
        self.trace_errors[tid]['METADATA']['missing_um'] = int(trace['um'].isna().sum()) + \
            trace[trace['um'] == '(?)'].shape[0]
        self.trace_errors[tid]['METADATA']['missing_dm'] = int(trace['dm'].isna().sum()) + trace[trace['dm'] == '(?)'].shape[0]
        self.trace_errors[tid]['METADATA']['missing_interface'] = int(trace['interface'].isna().sum()) + \
            trace[trace['interface'] == '(?)'].shape[0]

        trace['um'].fillna('(?)', inplace=True)
        trace['dm'].fillna('(?)', inplace=True)
        trace['interface'].fillna('(?)', inplace=True)
        return
    
    def get_children_rpcids(self, rpcids, rpcid):
        # get all rpcids with
        children_rpcids = []
        for id in rpcids:
            parent_id = id.rsplit('.', 1)[0]
            if parent_id == rpcid:
                children_rpcids.append(id)
        return children_rpcids
    
    # if dm name is (?), see if there is a duplicate record to
    # Note: assumes UM has already been corrected
    def fix_missing_dms(self, trace, rpcs):
        # if no missing values, return
        dms = rpcs['dm'].unique().tolist()
        if '(?)' not in dms:
            return rpcs
        
        # if duplicate row, see if we can rebuild missing value
        if rpcs.shape[0] == 2 and len(dms) > 1:
            dms.remove('(?)')
            rpcs['dm'] = dms[0]
        # if no duplicate rows or unable to fix missing values, 
        # check children rpcs for UM
        elif rpcs.shape[0] <= 2:
            # get children rpcids from rpcids
            children_rpcids = self.get_children_rpcids(trace['rpcid'].unique().tolist(), rpcs['rpcid'].tolist()[0])
            # if no children or duplicate records, unrecoverable missing value.
            if len(children_rpcids) == 0:
                rpcs['dm'] = 'UNKNOWN'
            else:
                children_rows = trace[trace['rpcid'].isin(children_rpcids)]
                children_ums = children_rows['um'].unique().tolist()
                # should only have one um for the children
                if '(?)' in children_ums: children_ums.remove('(?)')
                if len(children_ums) == 1:
                    rpcs['dm'] = children_ums[0]
                else:
                    rpcs['dm'] = 'UNKNOWN'
        # Note: When CP error, cannot definitively fill in DM services 
        # even using timestamp information
        return rpcs 
    
    def update_global_error_counters(self, cp_affected, dl_affected):
        if not cp_affected and not dl_affected:
            self.n_unaffected_traces_in_file += 1
        if cp_affected:
            self.n_cp_affected_traces_in_file += 1
        if dl_affected:
            self.n_dl_affected_traces_in_file += 1
        return

    # check if upstream path exists
    # return true if data loss detected, fix data loss if mode requires it. 
    def check_for_data_loss(self, tid, rpcid, root_rpcids, all_rpcids):
        dl_affected = False
        if rpcid in root_rpcids:
            return dl_affected
        
        # check if parent call exists 
        parent_rpcid = rpcid.rsplit('.', 1)[0]
        if parent_rpcid not in root_rpcids and parent_rpcid not in all_rpcids:
            # missing record that results in hole in trace
            # self.trace_errors[tid]['METADATA']['missing_rpcids'] += 1
            dl_affected = True

        if (dl_affected and 'rebuild' in self.args['mode']):
            self.correct_data_loss(tid, rpcid, root_rpcids, all_rpcids)

        return dl_affected
    
    # recursively add spans for missing upstream rpcids
    # if we have not created a span for the parent, recursively add spans until we reach an existing span. Stop when no remaining '.' in rpcid -- output error in this case for disconnected tree.
    def correct_data_loss(self, tid, rpcid, root_rpcids, all_rpcids):
        parent_rpcid = rpcid.rsplit('.', 1)[0]

        # NOTE: this is expensive, can be optimized!
        rpcids = self.open_traces[self.open_traces['traceid'] == tid]
        this_rpcid = rpcids[rpcids['rpcid'] == rpcid]

        # assuming only one child UM -- because no CP in this case
        childs_um = this_rpcid['um'].unique().tolist()[0]
        # while parent rpcid does not exist and parent rpcid has a .
        while (parent_rpcid not in root_rpcids and parent_rpcid not in all_rpcids and '.' in parent_rpcid):
            self.trace_errors[tid]['METADATA']['missing_rpcids'] += 1
            # create a span for the parent.
            # check if grandparent exists, if so it's DM is this rpc's UM
            grandparent_rpcid = parent_rpcid.rsplit('.', 1)[0]
            this_um = '(?)'
            if grandparent_rpcid in all_rpcids:
                this_um = rpcids[rpcids['rpcid'] == grandparent_rpcid]['dm'].unique().tolist()[0]
            new_row = pd.DataFrame({'Unnamed: 0': [1.0], 'traceid': [tid], 'timestamp': [0], 'rpcid': [parent_rpcid], 'um': [
                                   this_um], 'rpctype': ['UNKNOWN'], 'dm': [childs_um], 'interface': ['UNKNOWN'], 'rt': [0]})
                  
            self.processed_traces_rebuild = self.add_rows_to_processed_traces(new_row, self.processed_traces_rebuild)

            # reset child to unknown
            childs_um = 'UNKNOWN'
            # check parent of this node 
            parent_rpcid = grandparent_rpcid
        return
    
    def key(self, rpcid):
        return rpcid.count('.')

    def sort_rpcids(self, rpcids):
        # sort first by # of periods then by last digit 
        return sorted(rpcids, key=self.key)

    def get_process_id(self, name):
        # check if name is already assigned a process id
        if name in self.p_map:
            return self.p_map[name]
        # if not, create a new process object for this name
        pid = 'p' + str(len(self.processes.keys()) + 1)
        self.processes[pid] = {}
        self.processes[pid]['serviceName'] = name
        self.p_map[name] = pid
        return pid
    
    def start_trace_file(self, trace_id, mode):
        # start json object for trace
        beginning_of_file = '{"data": [{"traceID": "' + \
            str(trace_id) + '", "spans": ['
        # create file named after trace id, write to it
        with open(self.modes_output_dir(mode) + str(trace_id) + '.json', 'w') as outfile:
            outfile.write(beginning_of_file)

        return 

    def modes_output_dir(self, mode):
        return self.output_dir + 'output-' + mode + '/'

    # end trace file, if it exists
    def end_trace_file(self, trace_id, mode):
        # close json object in file 
        if (os.path.exists(self.modes_output_dir(mode) + str(trace_id) + '.json')):
            end_of_file = '], "processes":' + json.dumps(self.processes) + ', "warnings": null}], "total": 0, "limit": 0, "offset": 0, "errors": null }'
            # append end of json object to file
            with open(self.modes_output_dir(mode) + str(trace_id) + '.json', 'a+') as outfile:
                outfile.write(end_of_file)
        return 
    
    def build_span(self, trace_id, rpcid, name, interface, mode, root=False, first_span=False):
        # if root, don't include references 
        if first_span:
            beginning = '{'
        else:
            beginning = ', {'

        if root:
            parent_rpcid = rpcid
            if '.' not in rpcid:
                parent_rpcid = '-' + parent_rpcid
                
            span_data = beginning + ' "traceID": "' + \
                str(trace_id) + '", "spanID": "' + parent_rpcid + \
                '", "operationName": "' + str(interface) +  \
                '", "references": [], "startTime": 1, "duration": 1, "processID": "' + \
                str(self.get_process_id(name)) + '"}'
        else:
            span_data = beginning + ' "traceID": "' + \
                str(trace_id) + '", "spanID": "' + str(rpcid) + \
                '", "operationName": "' + str(interface) +  \
                '", "references": [{"refType": "CHILD_OF",  "traceID": "' + str(
                    trace_id) + '", "spanID": "' + str(rpcid) + '"}], "startTime": 1, "duration": 1,  "tags": [], "logs": [], "processID": "' + str(self.get_process_id(name)) + '"}'

        # append span to file
        with open(self.modes_output_dir(mode) + str(trace_id) + '.json', 'a+') as outfile:
            outfile.write(span_data)
        return False

    def output_error_stats(self):
        # check for error-stats directory, create it if it dne
        if not os.path.exists(self.modes_output_dir('rebuild') + 'error-stats'):
            os.makedirs(self.modes_output_dir('rebuild') + 'error-stats')

        # write cp_errors to a file, only rebuild mode
        if 'rebuild' in self.args['mode']:
            with open(self.modes_output_dir('rebuild') + 'error-stats/' + str(self.thread_ctr) + '-errors-' + str(self.current_output_file_id) + '.json', "w") as f:
                json.dump(self.trace_errors, f)
        for mode in self.args['mode']:
            # check for error-stats directory, create it if it dne
            if not os.path.exists(self.modes_output_dir(mode) + 'error-stats'):
                os.makedirs(self.modes_output_dir(mode) + 'error-stats')
                
            # write global counters to a file (update if existing file)
            with open(self.modes_output_dir(mode) + 'error-stats/' + str(self.thread_ctr) + '-global-stats-' + str(self.current_output_file_id) + '.json', "w") as f:
                f.write('Number of traces: ' + str(self.n_traces) + "\n")
                f.write('Number of unaffected traces: ' +
                        str(self.n_unaffected_traces) + "\n")
                f.write('Number of CP affected traces: ' +
                        str(self.n_cp_affected_traces) + "\n")
                f.write('Number of DL affected traces: ' +
                        str(self.n_dl_affected_traces) + "\n")
                f.write('Number of contradicting path value traces: ' +
                        str(self.trace_with_bad_um_rpcids) + "\n")
        return

# For finding all root rpcids -- linear time complexity
class RpcidTrie:
    class TrieNode:
        def __init__(self):
            self.children = {}
            self.is_end_of_rpcid = False

    def __init__(self):
        self.root = self.TrieNode()

    def insert(self, rpcid):
        node = self.root
        for part in rpcid.split('.'):
            if part not in node.children:
                node.children[part] = self.TrieNode()
            node = node.children[part]
        # used to tell if this node is end of an rpcid (when traversing from
        # root) 
        node.is_end_of_rpcid = True

    def find_roots(self, node=None, current_path=None, root_rpcids=None, has_ancestor_end=False):
        if root_rpcids is None:
            root_rpcids = []
        if current_path is None:
            current_path = []
        if node is None:
            node = self.root

        if node.is_end_of_rpcid and not has_ancestor_end:
            root_rpcids.append(".".join(current_path))

        for part, child in node.children.items():
            self.find_roots(
                child, current_path + [part], root_rpcids, has_ancestor_end or node.is_end_of_rpcid)

        return root_rpcids