import getpass
import json
import logging
import os
import sys

import pandas as pd

def get_key_interval_dicts_from_json(key_intervalname_json_path, logger):
    with open(key_intervalname_json_path, 'r') as json_path_open:
        json_data = json.load(json_path_open)
    return json_data


def all_tsv_to_df(tsv_path, logger):
    logger.info('all_tsv_to_df open: %s' % tsv_path)
    data_dict = dict()
    with open(tsv_path, 'r') as tsv_open:
        i = 0
        for line in tsv_open:
            line = line.strip('\n')
            line_split = line.split('\t')
            data_dict[i] = line_split
            i += 1
    logger.info('data_dict=\n%s' % data_dict)
    df = pd.DataFrame.from_dict(data_dict, orient='index')
    logger.info('df=\n%s' % df)
    return df


def picard_select_tsv_to_df(stats_path, select, logger):
    read_header = False
    data_dict = dict()
    if not os.path.exists(stats_path):
        logger.info('the stats file %s do not exist, so return None' % stats_path)
        return None
    logger.info('stats_path=%s' % stats_path)
    with open(stats_path, 'r') as stats_open:
        i = 0
        for line in stats_open:
            line = line.strip('\n')
            logger.info('line=\n%s' % line)
            if line.startswith('#'):
                continue
            line_split = line.split('\t')
            logger.info('len(line_split)=%s' % str(len(line_split)))
            if not read_header and len(line_split) > 1:
                if select == line_split[0]:
                    header = line_split
                    read_header = True
            elif read_header and len(line_split) == 1:
                df_index = list(range(len(data_dict)))
                df = pd.DataFrame.from_dict(data_dict, orient='index')
                logger.info('df=\n%s' % df)
                df.columns = header
                return df
            elif read_header and len(line_split) > 0:
                if len(line_split) == len(header):
                    logger.info('store line=\n%s' % line)
                    data_dict[i] = line_split
                    i += 1
            elif not read_header and len(line_split) == 1:
                continue
            else:
                logger.info('strange line: %s' % line)
                sys.exit(1)
    if not read_header:
        logger.info('bam file was probably too small to generate stats as header not read: %s' % stats_path)
        return None
    logger.debug('no data saved to df')
    sys.exit(1)
    return None


def picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger):
    select = 'CATEGORY'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectBaseDistributionByCycle_to_df(stats_path, logger):
    select = 'READ_END'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectInsertSizeMetrics_metrics_to_df(stats_path, logger):
    select = 'MEDIAN_INSERT_SIZE'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectInsertSizeMetrics_histogram_to_df(stats_path, logger):
    select = 'insert_size'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    if df is not None:
        keep_column_list = ['insert_size', 'All_Reads.fr_count', 'All_Reads.rf_count', 'All_Reads.tandem_count']
        drop_column_list = [ column for column in df.columns if column not in keep_column_list]
        needed_column_list = [ column for column in keep_column_list if column not in df.columns ]
        #drop readgroup specific columns as the bam is already one readgroup
        logger.info('pre drop df=\n%s' % df)
        df.drop(drop_column_list, axis=1, inplace=True)
        logger.info('post drop df=\n%s' % df)
        #add columns that could be present in other files
        for needed_column in needed_column_list:
            df[needed_column] = None
    return df


def picard_MeanQualityByCycle_to_df(stats_path, logger):
    select = 'CYCLE'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectQualityYieldMetrics_to_df(stats_path, logger):
    select = 'TOTAL_READS'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_QualityScoreDistribution_to_df(stats_path, logger):
    select = 'QUALITY'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectGcBiasMetrics_to_df(stats_path, logger):
    select = 'ACCUMULATION_LEVEL'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectSequencingArtifactMetrics_to_df(stats_path, logger):
    select = 'SAMPLE_ALIAS'
    def = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectWgsMetrics_metrics_to_df(stats_path, logger):
    select = 'GENOME_TERRITORY'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectWgsMetrics_histogram_to_df(stats_path, logger):
    select = 'coverage'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CalculateHsMetrics_to_df(stats_path, logger):
    select = 'BAIT_SET'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def picard_CollectOxoGMetrics_to_df(stats_path, logger):
    select = 'SAMPLE_ALIAS'
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def do_picard_metrics(uuid, stats_path, input_state, bam, fasta, engine, logger, metrics_type, wxs_dict = None, vcf = None):
    stats_dir = os.path.dirname(stats_path)
    stats_name = os.path.basename(stats_path)
    stats_base, stats_ext = os.path.splitext(stats_name)

    df_list = list()
    table_name_list = list()
    ###CASE FOR METRICS###
    if metrics_type == 'CollectAlignmentSummaryMetrics':
        table_name = 'picard_CollectAlignmentSummaryMetrics'
        df = picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)
    elif metrics_type == 'CollectJumpingLibraryMetrics':
        pass
    elif metrics_type == 'CollectVariantCallingMetrics':
        pass
    elif metrics_type == 'CollectWgsMetricsFromQuerySorted':
        pass
    elif metrics_type == 'CollectWgsMetricsFromSampledSites':
        pass
    elif metrics_type == 'CollectWgsMetricsWithNonZeroCoverage':
        pass
    elif metrics_type == 'EstimateLibraryComplexity':
        pass
    elif metrics_type == 'CollectMultipleMetrics':
        table_name = 'picard_CollectAlignmentSummaryMetrics'
        stats_file = stats_base + '.alignment_summary_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

        table_name = 'picard_CollectBaseDistributionByCycle'
        stats_file = stats_base + 'base_distribution_by_cycle_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectBaseDistributionByCycle_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

        table_name = 'picard_CollectInsertSizeMetric'
        stats_file = stats_base + '.insert_size_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectInsertSizeMetrics_metrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_metrics'
            table_name_list.append(table_name)
        df = picard_CollectInsertSizeMetrics_histogram_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_histogram'
            table_name_list.append(table_name)

        table_name = 'picard_MeanQualityByCycle'
        stats_file = stats_base + '.quality_by_cycle_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_MeanQualityByCycle_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

        table_name = 'picard_QualityScoreDistribution'
        stats_file = stats_base + '.quality_distribution_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_QualityScoreDistribution_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

        table_name = 'picard_CollectGcBiasMetrics'
        stats_file = stats_base + '.gc_bias.summary_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectGcBiasMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_summary'
            table_name_list.append(table_name)
        stats_file = stats_base + '.gc_bias.detail_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectGcBiasMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_detail'
            table_name_list.append(table_name)

        table_name = 'picard_CollectQualityYieldMetrics'
        stats_file = stats_base + '.quality_yield_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectQualityYieldMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)
            
        table_name = 'picard_CollectSequencingArtifactMetrics'
        stats_file = stats_base + '.pre_adapter_detail_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectSequencingArtifactMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_detail'
            table_name_list.append(table_name)
        stats_file = stats_base + '.pre_adapter_summary_metrics'
        stats_path = os.path.join(stats_dir, stats_file)
        df = picard_CollectSequencingArtifactMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_summary'
            table_name_list.append(table_name)

    elif metrics_type == 'CollectOxoGMetrics':
        table_name = 'picard_' + metrics_type
        df = picard_CollectOxoGMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

    elif metrics_type == 'CollectWgsMetrics':
        table_name = 'picard_' + metrics_type
        df = picard_CollectWgsMetrics_metrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)

        df = picard_CollectWgsMetrics_histogram_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name += '_histogram'
            table_name_list.append(table_name)
    elif metrics_type == 'CalculateHsMetrics':
        table_name = 'picard_' + metrics_type
        df = picard_CalculateHsMetrics_to_df(stats_path, logger)
        if df is not None:
            df_list.append(df)
            table_name_list.append(table_name)
    else:
        logger.debug('Unknown metrics_type: %s' % metrics_type)
        sys.exit(1)
    for i, df in enumerate(df_list):
        logger.info('df_list enumerate i=%s:' % i)
        df['uuid'] = uuid
        df['bam'] = bam
        df['input_state'] = input_state
        df['fasta'] = fasta
        if vcf is not None:
            df['vcf'] = vcf
        table_name = table_name_list[i]
        df.to_sql(table_name, engine, if_exists='append')
    return
