#!/usr/bin/env python

import argparse
import logging
import sys

import sqlalchemy

import metrics.bam_stats as bam_stats
import metrics.picard_buildbamindex as picard_buildbamindex
import metrics.picard_collectoxogmetrics as picard_collectoxogmetrics
import metrics.picard_collectsequencingartifactmetrics as picard_collectsequencingartifactmetrics
import metrics.picard_markduplicates as picard_markduplicates
import metrics.picard_mergesamfiles as picard_mergesamfiles
import metrics.picard_sortsam as picard_sortsam
import metrics.picard_validatesamfile as picard_validatesamfile
from metrics.picard_calculatehsmetrics_gdc import picard_calculatehsmetrics as picard_calculatehsmetrics_gdc
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
    parser.add_argument('--db_snp',
                        required = False
    )
    parser.add_argument('--outbam_name',
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
    metric_name = args.metric_name
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
    #     picard_calculatehsmetrics_target(uuid, bam, input_state, json_path, interval_dir, engine, logger, wxs_dict = wxs_dict)
    # elif metric_name == 'CollectHsMetrics_tcga':
    #     bam = get_param(args, 'bam')
    #     fasta = get_param(args, 'fasta')
    #     json_path = get_param(args, 'json_path')
    #     interval_dir = get_param(args, 'interval_dir')
    #     wxs_dict['bait_intervals_path'] = bait_intervals_path
    #     wxs_dict['target_intervals_path'] = target_intervals_path
    #     picard_calculatehsmetrics_tcga(uuid, bam, input_state, json_path, interval_dir, engine, logger, wxs_dict = wxs_dict)
    if metric_name == 'CollectHsMetrics_gdc':
        bam = get_param(args, 'bam')
        bam_library_kit_json_path = get_param(args, 'bam_library_kit_json_path')
        input_state = get_param(args, 'input_state')
        orig_bam_name = get_param(args, 'outbam_name')
        fasta = get_param(args, 'fasta')
        readgroup_json_path = get_param(args, 'readgroup_json_path')
        picard_calculatehsmetrics_gdc(uuid, bam, readgroup_json_path, bam_library_kit_json_path, orig_bam_name, input_state, engine, logger)
    elif metric_name == 'CollectAlignmentSummaryMetrics':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        fasta = get_param(args, 'fasta')
        bam_stats.do_picard_metrics(uuid, bam, input_state, fasta, engine, logger, 'CollectAlignmentSummaryMetrics')
    elif metric_name == 'CollectMultipleMetrics':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        vcf = get_param(args, 'vcf')
        fasta = get_param(args, 'fasta')
        bam_stats.do_picard_metrics(uuid, bam, input_state, fasta, engine, logger, 'CollectMultipleMetrics', vcf = vcf)
    elif metric_name == 'CollectOxoGMetrics':
        bam = get_param(args, 'bam')
        db_snp = get_param(args, 'db_snp')
        input_state = get_param(args, 'input_state')
        fasta = get_param(args, 'fasta')
        picard_collectoxogmetrics.picard_collectoxogmetrics(uuid, bam, db_snp, fasta, input_state, engine, logger)
    elif metric_name == 'CollectWgsMetrics':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        fasta = get_param(args, 'fasta')
        bam_stats.do_picard_metrics(uuid, bam, input_state, fasta, engine, logger, 'CollectWgsMetrics')
    elif metric_name == 'MarkDuplicates':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_markduplicates.bam_markduplicates(uuid, bam, input_state, engine, logger)
    elif metric_name == 'MarkDuplicatesWithMateCigar':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_markduplicates.bam_markduplicateswithmatecigar(uuid, bam, input_state, engine, logger)
    elif metric_name == 'ValidateSamFile':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_validatesamfile.picard_validatesamfile(uuid, bam, input_state, engine, logger)
    else:
        sys.exit('No recognized tool was selected')
        
    return


if __name__ == '__main__':
    main()
