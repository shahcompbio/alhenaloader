import pandas as pd
import datetime


def load_analysis(analysis_id, data, metadata_record, projects, es):
    load_data(data, analysis_id, es)

    metadata_record['cell_count'] = data['hmmcopy_metrics'].shape[0]

    es.load_record(metadata_record, analysis_id, es.ANALYSIS_ENTRY_INDEX)

    missing_labels = es.get_missing_labels()
    for label in missing_labels:
        es.add_label(label)

    es.add_analysis_to_projects(analysis_id, projects)


def clean_analysis(analysis_id, es):
    clean_data(analysis_id, es)

    es.delete_record_by_id(es.ANALYSIS_ENTRY_INDEX, analysis_id)

    es.remove_analysis_from_projects(analysis_id)

def clean_data(analysis_id, es):
    for data_type, get_data in GET_DATA.items():
        es.delete_index(f"{analysis_id.lower()}_{data_type}")

def process_analysis_entry(analysis_id, library_id, sample_id, description, metadata):
    record = { 
        **metadata
    }

    record['timestamp'] = datetime.datetime.now().isoformat()
    record["dashboard_id"] = analysis_id
    record["jira_id"] = analysis_id
    record["dashboard_type"] = "single"
    record["library_id"] = library_id
    record["sample_id"] = sample_id
    record["description"] = description

    return record

def load_analysis_entry(analysis_id, library_id, sample_id, description, metadata, es):
    record = process_analysis_entry(analysis_id, library_id, sample_id, description, metadata)

    es.load_record(record, analysis_id, es.ANALYSIS_ENTRY_INDEX)


def load_data(data, analysis_id, es):
    """Load dataframes"""

    for data_type, get_data in GET_DATA.items():
        df = get_data(data)
        es.load_df(df, f"{analysis_id.lower()}_{data_type}")


def get_qc_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_metrics']
    data['percent_unmapped_reads'] = data["unmapped_reads"] / data["total_reads"]
    data['is_contaminated'] = data['is_contaminated'].apply(
        lambda a: {True: 'true', False: 'false'}[a])
    data.rename(columns={'clustering_order': 'order', 'condition': 'experimental_condition'}, inplace=True)
    return data


def get_segs_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_segs'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_bins_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_reads'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_gc_bias_data(hmmcopy_data):
    data = hmmcopy_data['gc_metrics']

    gc_cols = list(range(101))
    gc_bias_df = pd.DataFrame(columns=['cell_id', 'gc_percent', 'value'])
    for n in gc_cols:
        new_df = data.loc[:, ['cell_id', str(n)]]
        new_df.columns = ['cell_id', 'value']
        new_df['gc_percent'] = n
        gc_bias_df = gc_bias_df.append(new_df, ignore_index=True)

    return gc_bias_df


GET_DATA = {
    f"qc": get_qc_data,
    f"segs": get_segs_data,
    f"bins": get_bins_data,
    f"gc_bias": get_gc_bias_data,
}


chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}


def create_chrom_number(chromosomes):
    chrom_number = chromosomes.map(lambda a: chr_prefixed.get(a, a))
    return chrom_number
