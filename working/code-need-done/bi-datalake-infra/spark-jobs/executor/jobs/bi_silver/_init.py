# %livy.pyspark
spark.sql("drop  table IF EXISTS bi_silver.deal_quotations")
spark.sql("create database if not exists bi_silver")
spark.sql("use bi_silver")

# %livy.pyspark
import hashlib

import hashlib

def sha256_surrogate(*cols):
    parts = []

    for c in cols:
        if c is None:
            s = u"∅"
        else:
            # đảm bảo là unicode
            if not isinstance(c, unicode):
                s = unicode(c)
            else:
                s = c

            s = s.strip().lower()

        parts.append(s)

    raw = u"|".join(parts)

    # 🔑 DÒNG QUAN TRỌNG
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    
from pyspark.sql.types import StringType

spark.udf.register(
    "sha256_id",
    sha256_surrogate,
    StringType()
)

# from pyspark.sql.functions import pandas_udf
# import pandas as pd
# import hashlib

# @pandas_udf("string")
# def sha256_pandas(s: pd.Series) -> pd.Series:
#     return s.fillna("∅").str.strip().str.lower().apply(
#         lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest()
#     )