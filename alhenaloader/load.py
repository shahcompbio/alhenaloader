from scgenome.loaders.align import load_align_data
from scgenome.loaders.hmmcopy import load_hmmcopy_data
from scgenome.loaders.annotation import load_annotation_data
import pandas as pd

GET_DATA = {
    f"qc": get_qc_data,
    f"segs": get_segs_data,
    f"bins": get_bins_data,
    f"gc_bias": get_gc_bias_data,
}


def clean_data(dashboard_id, es):
    for data_type, get_data in GET_DATA.items():
        es.delete_index(f"{dashboard_id.lower()}_{data_type}")


def load_qc_from_dirs(alignment, hmmcopy, annotation):
    """Opens data associated with alignment, hmmcopy, annotation and returns dict of dataframes"""

    qc_data = load_align_data(alignment)

    for table_name, data in load_hmmcopy_data(hmmcopy).items():
        qc_data[table_name] = data

    for table_name, data in load_annotation_data(annotation).items():
        qc_data[table_name] = data

    return qc_data


def load_dashboard_entry(dashboard_id, library_id, sample_id, description, metadata, es):
    record = {
        **metadata
    }

    record["dashboard_id"] = dashboard_id
    record["jira_id"] = dashboard_id
    record["dashboard_type"] = "single"
    record["library_id"] = library_id
    record["sample_id"] = sample_id
    record["description"] = description

    es.load_record(record, dashboard_id, es.DASHBOARD_ENTRY_INDEX)


def load_data(data, dashboard_id, es):
    """Load dataframes"""

    for data_type, get_data in GET_DATA.items():
        df = get_data(data)
        es.load_df(df, f"{dashboard_id.lower()}_{data_type}")


def get_qc_data(hmmcopy_data):
    data = hmmcopy_data['annotation_metrics']
    data['percent_unmapped_reads'] = data["unmapped_reads"] / data["total_reads"]
    data['is_contaminated'] = data['is_contaminated'].apply(
        lambda a: {True: 'true', False: 'false'}[a])
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


chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}


def create_chrom_number(chromosomes):
    chrom_number = chromosomes.map(lambda a: chr_prefixed.get(a, a))
    return chrom_number
