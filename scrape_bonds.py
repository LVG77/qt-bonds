import urllib.request
from io import BytesIO
import pandas as pd
import numpy as np
from typing import Dict, List

url = "https://www.questrade.com/docs/librariesprovider7/default-document-library/questrade_bonds_list_excel.xlsx"
headers = {"User-Agent": "Mozilla/5.0"}

df_names = {
    "GIC LESS THEN 1 YEAR": "gic_1y_less",
    "GIC 1-5 YEAR": "gic_1y_more_5y",
    "GIC 1-6 YEAR": "gic_1y_more_6y",
    "MUNICIPAL": "municipal",
    "COUPONS": "coupon",
    "CORPORATE": "corporate",
    "HIGH YIELD": "high_yield",
    "PROVINCES": "provinces",
}


def get_tbl(
    xl_dict: Dict[str, pd.DataFrame], names_map: Dict[str, str]
) -> Dict[str, pd.DataFrame]:
    "Loop though `xl_dict` and format the dfs"
    dict_out = {}
    for sh, df in xl_dict.items():
        rows_to_drop = []
        for row in df.itertuples():
            if row[1] == "Today:":
                today = row[2]
                rows_to_drop.append(row[0])
            elif row[1] == "Settlement:":
                settle_date = row[2]
                rows_to_drop.append(row[0])
            elif row[2] is np.nan and row[3] is np.nan:
                rows_to_drop.append(row[0])
        df_out = df.drop(rows_to_drop, axis=0)
        df_out.columns = df_out.iloc[0]
        df_out = df_out.iloc[1:]
        dict_out[names_map[sh]] = df_out
        df_out["quote_date"] = today
        df_out["settle_date"] = settle_date
    return dict_out

def fix_1y_more_tbl(df: pd.DataFrame) -> pd.DataFrame:
        col_mask = [c for c in df.columns if c.endswith("_y")]
        df[col_mask] = df[col_mask].apply(lambda c: c.str.replace("%", ""))
        df[col_mask] = df[col_mask].apply(
            lambda c: pd.to_numeric(c, errors="coerce")
        )
        df["Financial Institution"] = df["Financial Institution"].ffill()
        return df.query("~`Financial Institution`.isin(['Heading', 'Compound Frequency'])")

def fix_dfs(dict_df: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    for name, df in dict_df.items():
        if name == "gic_1y_less":
            if df.shape[1] == 12: 
                df.columns = [i for i in range(12)]
                if df[9].isnull().sum() > 0.85*df.shape[0]: df.drop(columns=9, inplace=True)
            df.columns = [
                "Financial Institution",
                "Redeemable",
                "Minimum Deposit",
                "30_days",
                "60_days",
                "90_days",
                "120_days",
                "180_days",
                "270_days",
                "quote_date",
                "settle_date",
            ]
            col_mask = [c for c in df.columns if c.endswith("_days")]
            df[col_mask] = df[col_mask].apply(lambda c: c.str.replace("%", ""))
            df[col_mask] = df[col_mask].apply(
                lambda c: pd.to_numeric(c, errors="coerce")
            )
            df["Financial Institution"] = df["Financial Institution"].ffill()
        elif name == "gic_1y_more_5y":
            if df.shape[1] > 13:
                df.columns = [i if pd.isna(df.iloc[:, i].name) else df.iloc[:, i].name for i in range(df.shape[1])]
                for col in df.columns:
                    if df[col].isnull().sum() > 0.85*df.shape[0] or (df[col] == '-').sum() > 0.85*df.shape[0]:
                        df.drop(columns=col, inplace=True)
            df.columns = [
                "Financial Institution",
                "Compound Frequency",
                "Payment Frequency",
                "Redeemable",
                "Redeemable Term",
                "Minimum Deposit",
                "1_y",
                "2_y",
                "3_y",
                "4_y",
                "5_y",
                "quote_date",
                "settle_date",
            ]
            dict_df[name] = fix_1y_more_tbl(df)
        elif name == "gic_1y_more_6y":
            if df.shape[1] > 14:
                df.columns = [i if pd.isna(df.iloc[:, i].name) else df.iloc[:, i].name for i in range(df.shape[1])]
                for col in df.columns:
                    if df[col].isnull().sum() > 0.85*df.shape[0] or (df[col] == '-').sum() > 0.85*df.shape[0]:
                        df.drop(columns=col, inplace=True)
            df.columns = [
                "Financial Institution",
                "Compound Frequency",
                "Payment Frequency",
                "Redeemable",
                "Redeemable Term",
                "Minimum Deposit",
                "1_y",
                "2_y",
                "3_y",
                "4_y",
                "5_y",
                "6_y",
                "quote_date",
                "settle_date",
            ]
            dict_df[name] = fix_1y_more_tbl(df)
    return dict_df


if __name__ == "__main__":
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as r:
        xl = r.read()

    xl_data = pd.read_excel(BytesIO(xl), sheet_name=None)
    out = get_tbl(xl_data, df_names)
    out_fixed = fix_dfs(out)
    for name, df in out_fixed.items():
        df.to_csv(name + ".csv", index=False)
