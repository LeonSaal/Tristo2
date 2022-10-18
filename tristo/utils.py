from hashlib import sha1
from typing import List

import pandas as pd


def count_occ(dfs: List[pd.DataFrame], pat: str) -> int:
    return int(
        sum(
            [
                df.reset_index()
                .apply(lambda x: x.astype(str).str.contains(pat))
                .to_numpy()
                .sum()
                for df in dfs
            ]
        )
    )


def hashf(x: str):
    return sha1(x.encode("utf-8")).hexdigest()


def crop_text(text: str, n_chars: int = 8):
    text = text.replace("\n", "")
    if n_chars < 4:
        n_chars = 4
    if len(text) > n_chars:
        text = text[: n_chars - 3] + "..."

    return f"{text:.<{n_chars}}"
