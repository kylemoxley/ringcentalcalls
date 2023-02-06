import pandas as pd

def clean_columns(df: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    It cleans the column names of a dataframe.
    
    Args:
      df (pd.core.frame.DataFrame): pd.core.frame.DataFrame
    
    Returns:
      A dataframe with the columns cleaned up.
    """

    replace_dict = {
        " ": "_",
        "-": "_",
        "__": "_",
        "&": "_",
        "$": "_",
        ">": "_",
        "<": "_",
        "/": "_",
        "'": "_",
        "___": "_"}

    def replace_all(text, dic):
        for i, j in dic.items():
            text = text.replace(i, j)
        return text.title()

    df.columns = [replace_all(col.strip(), replace_dict) for col in df.columns]

    return df
