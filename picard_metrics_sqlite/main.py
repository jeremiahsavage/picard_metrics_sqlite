#!/usr/bin/env python

import argparse
import logging
import os
import sys

import sqlalchemy

import .metrics import picard_collectalignmentsummarymetrics
import .metrics import picard_collectmultiplemetrics
import .metrics import picard_collectoxogmetrics
import .metrics import picard_markduplicates
import .metrics import picard_validatesamfile
#from metrics.picard_calculatehsmetrics_gdc import picard_calculatehsmetrics as picard_calculatehsmetrics_gdc
# from metrics.picard_calculatehsmetrics_tcga import picard_calculatehsmetrics as picard_calculatehsmetrics_tcga
# from metrics.picard_calculatehsmetrics_target import picard_calculatehsmetrics as picard_calculatehsmetrics_target

def get_param(args, param_name):
    if vars(args)[param_name] == None:
        sys.exit('--'+ param_name + ' is required')
    else:
        return vars(args)[param_name]
    return
    
def setup_logging(tool_name, args, uuid):
    logging.basicConfig(
        filename=os.path.join(uuid + '_' + tool_name + '.log'),
        level=args.level,
        filemode='w',
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S_%Z',
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    return logger

def main():
    parser = argparse.ArgumentParser('picard docker tool')

    # Logging flags.
    parser.add_argument('-d', '--debug',
        action = 'store_const',
        const = logging.DEBUG,
        dest = 'level',
        help = 'Enable debug logging.',
    )
    parser.set_defaults(level = logging.INFO)
    
    # Required flags.
    parser.add_argument('--input_state',
                        required = True
    )
    parser.add_argument('--metric_name',
                        required = True,
                        help = 'picard tool'
    )
    parser.add_argument('--stats_path',
                        required = True
    )
    parser.add_argument('--uuid',
                        required = True,
                        help = 'uuid string',
    )
    
    # Tool flags
    parser.add_argument('--bam_library_kit_json_path',
                        required = False
    )
    parser.add_argument('--bam',
                        required = False
    )
    parser.add_argument('--vcf',
                        required = False
    )
    parser.add_argument('--readgroup_json_path',
                        required = False
    )
    parser.add_argument('--fasta',
                        required = False
    )

    
    # setup required parameters
    args = parser.parse_args()
    input_state = args.input_state
    metric_name = args.metric_name
    stats_path = args.stats_path
    uuid = args.uuid

    logger = setup_logging('picard_' + metric_name, args, uuid)

    sqlite_name = uuid + '.db'
    engine_path = 'sqlite:///' + sqlite_name
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')


    # elif metric_name == 'CollectHsMetrics_target':
    #     bam = get_param(args, 'bam')
    #     fasta = get_param(args, 'fasta')
    #     json_path = get_param(args, 'json_path')
    #     interval_dir = get_param(args, 'interval_dir')
    #     wxs_dict['bait_intervals_path'] = bait_intervals_path
    #     wxs_dict['target_intervals_path'] = target_intervals_path
    #     picard_calculatehsmetrics_target.run(uuid, bam, input_state, json_path, interval_dir, engine, logger, wxs_dict = wxs_dict)
    # elif metric_name == 'CollectHsMetrics_tcga':
    #     bam = get_param(args, 'bam')
    #     fasta = get_param(args, 'fasta')
    #     json_path = get_param(args, 'json_path')
    #     interval_dir = get_param(args, 'interval_dir')
    #     wxs_dict['bait_intervals_path'] = bait_intervals_path
    #     wxs_dict['target_intervals_path'] = target_intervals_path
    #     picard_calculatehsmetrics_tcga.run(uuid, bam, input_state, json_path, interval_dir, engine, logger, wxs_dict = wxs_dict)
    # if metric_name == 'CollectHsMetrics_gdc':
    #     bam = get_param(args, 'bam')
    #     bam_library_kit_json_path = get_param(args, 'bam_library_kit_json_path')
    #     input_state = get_param(args, 'input_state')
    #     orig_bam_name = get_param(args, 'outbam_name')
    #     fasta = get_param(args, 'fasta')
    #     readgroup_json_path = get_param(args, 'readgroup_json_path')
    #     picard_calculatehsmetrics_gdc.run(uuid, bam, readgroup_json_path, bam_library_kit_json_path, orig_bam_name, input_state, engine, logger)
    if metric_name == 'CollectAlignmentSummaryMetrics':
        bam = get_param(args, 'bam')
        fasta = get_param(args, 'fasta')
        picard_collectalignmentsummarymetrics.run(uuid, stats_path, bam, fasta, input_state, engine, logger, metric_name)
    elif metric_name == 'CollectMultipleMetrics':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        vcf = get_param(args, 'vcf')
        fasta = get_param(args, 'fasta')
        picard_collectmultiplemetrics.run(uuid, stats_path, bam, fasta, vcf, input_state, engine, logger)
    elif metric_name == 'CollectOxoGMetrics':
        bam = get_param(args, 'bam')
        fasta = get_param(args, 'fasta')
        input_state = get_param(args, 'input_state')
        vcf = get_param(args, 'vcf')
        picard_collectoxogmetrics.run(uuid, stats_path, bam, fasta, vcf, input_state, engine, logger, metric_name)
    elif metric_name == 'CollectWgsMetrics':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        fasta = get_param(args, 'fasta')
        picard_collectwgsmetrics.run(uuid, bam, input_state, fasta, engine, logger, metric_name)
    elif metric_name == 'MarkDuplicates':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_markduplicates.run(uuid, stats_path, bam, input_state, engine, logger, metric_name)
    # elif metric_name == 'MarkDuplicatesWithMateCigar':
    #     bam = get_param(args, 'bam')
    #     input_state = get_param(args, 'input_state')
    #     picard_markduplicateswithmatecigar.run(uuid, bam, input_state, engine, logger)
    elif metric_name == 'ValidateSamFile':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_validatesamfile.run(uuid, stats_path, bam, input_state, engine, logger)
    else:
        sys.exit('No recognized tool was selected')
        
    return


if __name__ == '__main__':
    main()
