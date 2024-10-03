import argparse
import csv
import json
import os

CLI=argparse.ArgumentParser()
CLI.add_argument("filelist", nargs="*", default=[])
args = CLI.parse_args()

samplesize = {}
samplesize['unet3d'] = {}
samplesize['unet3d']['A100'] = 146600628
samplesize['unet3d']['H100'] = 146600628
samplesize['resnet50'] = {}
samplesize['resnet50']['A100'] = 114660
samplesize['resnet50']['H100'] = 114660
samplesize['cosmoflow'] = {}
samplesize['cosmoflow']['A100'] = 2828486
samplesize['cosmoflow']['H100'] = 2828486

#
# Collect the system.json files
#
systems = {}
for root, dirs, files in os.walk('.'):
    pathcomps = root.split("/")
    pathlen = len(pathcomps)
    if pathlen > 1:
        if pathcomps[pathlen - 1] == 'systems':
            vendor = pathcomps[2]
            if vendor not in systems:
                systems[vendor] = {}
            for f in files:
                if '.json' in f.casefold() and f[0] != '.':
                    sysname = f
                    sysname = sysname.removesuffix('.json')
                    sysname = sysname.removesuffix('.JSON')
                    systems[vendor][sysname] = root + '/' + f

#
# Collect the summary file info
#
results = {}
for root, dirs, files in os.walk('.'):
    if 'mlperf_storage_report.json' in files:
        pathcomps = root.split("/")
        pathlen = len(pathcomps)
        if pathlen > 1:
            vendor = pathcomps[2]
            if vendor not in results:
                results[vendor] = []
            type = 'unknown'
            if 'open' in pathcomps or 'Open' in pathcomps:
                type = 'Open'
            if 'closed' in pathcomps or 'Closed' in pathcomps:
                type = 'Closed'
            if 'results' in pathcomps:
                sysname = pathcomps[pathcomps.index('results') + 1]
                if vendor in systems and sysname in systems[vendor]:
                    #print(vendor, root + '/mlperf_storage_report.json', type, systems[vendor][sysname])
                    results[vendor].append( (vendor, root + '/mlperf_storage_report.json', type, sysname, systems[vendor][sysname]) )
                else:
                    print('Found results but no corresponding system.json file:', vendor, sysname)
                    for s in systems[vendor]:
                        print('        ', systems[vendor][s])
                    results[vendor].append( (vendor, root + '/mlperf_storage_report.json', type, '???', '???') )
            else:
                print('Could not find results:', vendor, root + '/mlperf_storage_report.json')
                results[vendor].append( (vendor, root + '/mlperf_storage_report.json', type, '???', '???') )
                print('Badly formed hierarchy: ', vendor)

#print("VENDOR, RESULTS")
#print("------, ----")
#for v in results:
#    for r in results[v]:
#        print(v, r)
#print("VENDOR, SYSTEM")
#print("------, ------")
#for v in systems:
#    for s in systems[v]:
#        print(v, s, systems[v][s])

rows = {}
for res in results:
    for vendor, resfile, type, sysname, sysfile in results[res]:

        #print(vendor, rowfile, type, sysfile)
        infile = open(resfile)
        perftest = []
        perftest = json.load(infile)
        summary = perftest['overall'] 
        infile.close()
        #print(vendor, summary)

        usable = 0
        raw = 0
        soltype = ''
        vendorname = vendor
        if sysfile != '???':
            tmp = open(sysfile)
            sysdesc = []
            sysdesc = json.load(tmp)
            if 'storage_system' in sysdesc:
                detail = sysdesc['storage_system']
                if 'usable_capacity_GiB' in detail:
                    usable = detail['usable_capacity_GiB']
                else:
                    print('Did not find usable_capacity_GiB:', vendor, sysfile)
                if 'raw_capacity_GiB' in detail:
                    raw = detail['raw_capacity_GiB']
                else:
                    print('Did not find usable_capacity_GiB:', vendor, sysfile)
                if 'solution_type' in detail:
                    soltype = detail['solution_type']
                else:
                    print('Did not find solution_type:', vendor, sysfile)
                if 'vendor_name' in detail:
                    vendorname = detail['vendor_name']
                else:
                    print('Did not find vendor_name:', vendor, sysfile)
            else:
                print('Did not find storage_system:', vendor, sysfile)
            tmp.close()
        else:
            print('sysfile was unset:', vendor, sysfile)
        if usable == 0:
            print('Could not find usable capacity:', vendor, sysfile)
        accel = summary['accelerator'].upper()
        model = summary['model']

        # Build up the structure for this result

        datapt = {}
        datapt['unet3d_train_num_accelerators'] = ''
        datapt['unet3d_dataset_size'] = ''
        datapt['unet3d_train_throughput_mean_MB_per_second'] = ''
        datapt['resnet50_train_num_accelerators'] = ''
        datapt['resnet50_dataset_size'] = ''
        datapt['resnet50_train_throughput_mean_MB_per_second'] = ''
        datapt['cosmoflow_train_num_accelerators'] = ''
        datapt['cosmoflow_dataset_size'] = ''
        datapt['cosmoflow_train_throughput_mean_MB_per_second'] = ''

        datapt['vendorname'] = vendorname
        datapt['sysname'] = sysname
        datapt['solution_type'] = soltype
        datapt['usable_gb'] = usable
        datapt['raw_gb'] = raw
        datapt['accelerator'] = accel
        datapt['type'] = type
        datapt['avail'] = ''

        name_num_accel = model + '_train_num_accelerators'
        val_num_accel = summary['train_num_accelerators']
        datapt[name_num_accel] = val_num_accel

        name_ds_size = model + '_dataset_size'
        val_ds_size = int(summary['num_files_train']) * int(summary['num_samples_per_file']) * \
            int(samplesize[model][accel]) / (1024*1024*1024)
        datapt[name_ds_size] = val_ds_size

        name_MBps = model + '_train_throughput_mean_MB_per_second'
        val_MBps = summary['train_throughput_mean_MB_per_second']
        datapt[name_MBps] = val_MBps

        name = vendor + sysname + accel
        if name not in rows:
            rows[name] = datapt
        if rows[name][name_num_accel] == '' or int(rows[name][name_num_accel]) < int(val_num_accel):
            rows[name][name_num_accel] = val_num_accel
            rows[name][name_ds_size] = val_ds_size
            rows[name][name_MBps] = val_MBps
        #print(name, rows[name])

#
# Build the .csv output file
#
fields = [
    ('Type',                'type'),
    ('Availability',        'avail'),
    ('Organization',        'vendorname'),
    ('System Name',         'sysname'),
    ('System Type',         'solution_type'),
    ('Scaling Units',       '???'),
    ('Usable Capacity',     'usable_gb'),
    ('Total Capacity',      'raw_gb'),
    ('Accelerator',         'accelerator'),
    ('# Host Nodes',        '???'),
    ('Code',                '???'),
    ('Logs',                '???'),
    ('# Accel',             'unet3d_train_num_accelerators'),
    ('Dataset Size (GiB)',  'unet3d_dataset_size'),
    ('MiB/s',               'unet3d_train_throughput_mean_MB_per_second'),
    ('# Accel',             'cosmoflow_train_num_accelerators'),
    ('Dataset Size (GiB)',  'cosmoflow_dataset_size'),
    ('MiB/s',               'cosmoflow_train_throughput_mean_MB_per_second'),
    ('# Accel',             'resnet50_train_num_accelerators'),
    ('Dataset Size (GiB)',  'resnet50_dataset_size'),
    ('MiB/s',               'resnet50_train_throughput_mean_MB_per_second')
]

outfile = open('Results.csv','w')
writer = csv.writer(outfile)
tmplist = []
for title, fld in fields:       # The headers row in the table
    tmplist.append(title)
writer.writerow(tmplist)
for row in rows:                # All the detail rows in the table
    tmplist = []
    for title, fld in fields:
        if fld == '???':
            tmplist.append('')
        else:
            tmplist.append(rows[row][fld])
    writer.writerow(tmplist)
outfile.close()
