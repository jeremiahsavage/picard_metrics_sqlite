#!/usr/bin/env python

import argparse
import logging
import os
import sys

import sqlalchemy

from .metrics import picard_collectalignmentsummarymetrics
from .metrics import picard_collecthsmetrics
from .metrics import picard_collectmultiplemetrics
from .metrics import picard_collectoxogmetrics
from .metrics import picard_collectwgsmetrics
from .metrics import picard_markduplicates
from .metrics import picard_validatesamfile

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
    parser.add_argument('--metric_path',
                        required = True
    )
    parser.add_argument('--uuid',
                        required = True,
                        help = 'uuid string',
    )

    # Tool flags
    parser.add_argument('--bam',
                        required = False
    )
    parser.add_argument('--bam_library',
                        required = False
    )
    parser.add_argument('--exome_kit',
                        required = False
    )
    parser.add_argument('--fasta',
                        required = False
    )
    parser.add_argument('--vcf',
                        required = False
    )

    # setup required parameters
    args = parser.parse_args()
    input_state = args.input_state
    metric_name = args.metric_name
    metric_path = args.metric_path
    uuid = args.uuid

    logger = setup_logging('picard_' + metric_name, args, uuid)

    sqlite_name = uuid + '.db'
    engine_path = 'sqlite:///' + sqlite_name
    engine = sqlalchemy.create_engine(engine_path, isolation_level='SERIALIZABLE')

    if metric_name == 'CollectAlignmentSummaryMetrics':
        bam = get_param(args, 'bam')
        if vars(args)['fasta'] == None:
            fasta = None
        else:
            fasta = vars(args)['fasta']
        picard_collectalignmentsummarymetrics.run(uuid, metric_path, bam, fasta, input_state, engine, logger, metric_name)
    elif metric_name == 'CollectHsMetrics':
        bam = get_param(args, 'bam')
        bam_library = get_param(args, 'bam_library')
        input_state = get_param(args, 'input_state')
        exome_kit = get_param(args, 'exome_kit')
        fasta = get_param(args, 'fasta')
        picard_collecthsmetrics.run(uuid, metric_path, bam, bam_library, exome_kit, fasta, input_state, engine, logger, metric_name)
    elif metric_name == 'CollectMultipleMetrics':
        bam = get_param(args, 'bam')
        fasta = get_param(args, 'fasta')
        input_state = get_param(args, 'input_state')
        vcf = get_param(args, 'vcf')
        picard_collectmultiplemetrics.run(uuid, metric_path, bam, fasta, vcf, input_state, engine, logger)
    elif metric_name == 'CollectOxoGMetrics':
        bam = get_param(args, 'bam')
        fasta = get_param(args, 'fasta')
        input_state = get_param(args, 'input_state')
        vcf = get_param(args, 'vcf')
        picard_collectoxogmetrics.run(uuid, metric_path, bam, fasta, vcf, input_state, engine, logger, metric_name)
    elif metric_name == 'CollectWgsMetrics':
        bam = get_param(args, 'bam')
        fasta = get_param(args, 'fasta')
        input_state = get_param(args, 'input_state')
        picard_collectwgsmetrics.run(uuid, metric_path, bam, fasta, input_state, engine, logger, metric_name)
    elif metric_name == 'MarkDuplicates':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_markduplicates.run(uuid, metric_path, bam, input_state, engine, logger, metric_name)
    # elif metric_name == 'MarkDuplicatesWithMateCigar':
    #     bam = get_param(args, 'bam')
    #     input_state = get_param(args, 'input_state')
    #     picard_markduplicateswithmatecigar.run(uuid, bam, input_state, engine, logger)
    # elif metric_name == 'FixMateInformation':
    #     bam = get_param(args, 'bam')
    #     input_state = get_param(args, 'input_state')
    #     picard_fixmateinformation.run(uuid, bam, input_state, engine, logger)
    elif metric_name == 'ValidateSamFile':
        bam = get_param(args, 'bam')
        input_state = get_param(args, 'input_state')
        picard_validatesamfile.run(uuid, metric_path, bam, input_state, engine, logger)
    else:
        sys.exit('No recognized tool was selected')
        
    return

if __name__ == '__main__':
    main()
