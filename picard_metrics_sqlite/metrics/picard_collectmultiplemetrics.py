import os

from .metrics_util import picard_select_tsv_to_df


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
    df = picard_select_tsv_to_df(stats_path, select, logger)
    return df


def run(uuid, stats_path, bam, fasta, vcf, input_state, engine, logger):
    stats_dir = os.path.dirname(stats_path)
    stats_name = os.path.basename(stats_path)
    stats_base, stats_ext = os.path.splitext(stats_name)

    df_list = list()
    table_name_list = list()

    
    table_name = 'picard_CollectAlignmentSummaryMetrics'
    stats_file = stats_base + '.alignment_summary_metrics'
    stats_path = os.path.join(stats_dir, stats_file)
    df = picard_CollectAlignmentSummaryMetrics_to_df(stats_path, logger)
    if df is not None:
        df_list.append(df)
        table_name_list.append(table_name)

    table_name = 'picard_CollectBaseDistributionByCycle'
    stats_file = stats_base + '.base_distribution_by_cycle_metrics'
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

    table_name = 'picard_CollectInsertSizeMetric'
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

    table_name = 'picard_CollectGcBiasMetrics'
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

    table_name = 'picard_CollectSequencingArtifactMetrics'
    stats_file = stats_base + '.pre_adapter_summary_metrics'
    stats_path = os.path.join(stats_dir, stats_file)
    df = picard_CollectSequencingArtifactMetrics_to_df(stats_path, logger)
    if df is not None:
        df_list.append(df)
        table_name += '_summary'
        table_name_list.append(table_name)
            
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
