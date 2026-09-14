import json
import logging

from typing import Any, Dict, List, Optional

from transform.schema_contract import ColumnSpec, SchemaContract

logger = logging.getLogger(__name__)


def unwrap_expr(src_name: str):
    """Go lop boc value-object cua Clarizen ve mot gia tri scalar dang chuoi.

    API tra ve ba dang boc, tat ca deu duoc doc vao mot cot string:
        {"currency": "VND", "value": 0.0}                 -> value
        {"unit": "Days", "value": 5.0}                    -> value
        {"durationType": ..., "unit": ..., "value": ...}  -> value
        {"id": "/C_WorkItemRegion/AMER"}                  -> id

    Chi parse JSON khi gia tri thuc su bat dau bang '{'. Kiem tra mot ky tu
    re hon nhieu so voi goi get_json_object tren ca 364 cot moi dong.
    """
    from pyspark.sql import functions as F

    col = F.col(f"`{src_name}`")
    return F.when(
        col.startswith("{"),
        F.coalesce(
            F.get_json_object(col, "$.value"),
            F.get_json_object(col, "$.id"),
        )
    ).otherwise(col)


def cast_expr(spec: ColumnSpec):
    """Bieu thuc ep kieu theo dung khai bao trong contract."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    raw = unwrap_expr(spec.src_name)

    if spec.logical == "STRING":
        casted = raw
    elif spec.logical == "DOUBLE":
        # Executable va OnCriticalPath khai bao DOUBLE nhung API tra true/false.
        # Spark cast('true' AS double) ra null - im lang mat sach du lieu cot do.
        # Quy ve 1.0/0.0 de giu duoc gia tri ma van dung kieu contract da khai bao.
        casted = F.coalesce(
            raw.cast("double"),
            F.when(F.lower(raw) == "true", F.lit(1.0))
             .when(F.lower(raw) == "false", F.lit(0.0))
        )
    elif spec.logical == "BOOLEAN":
        casted = raw.cast("boolean")
    elif spec.logical == "DATE":
        # Du lieu that co dang 2026-09-08T00:00:00.0000000 - 7 chu so thap phan
        # giay, ngoai chuan ISO thong thuong. Cat 10 ky tu dau nen khong phu
        # thuoc parser va khong bi lech timezone.
        casted = F.to_date(F.substring(raw, 1, 10))
    else:
        precision, scale = spec.decimal_precision_scale
        casted = raw.cast(DecimalType(precision, scale))

    return casted.alias(spec.name)


def conform(df_str, contract: SchemaContract, include_raw_payload: bool = True):
    """Bien DataFrame toan string thanh DataFrame dung y het contract.

    Schema tra ve la mot ham thuan cua file .sql - khong phu thuoc du lieu
    trong batch. Day la tinh chat khien Parquet luon khop DDL Trino.
    """
    from pyspark.sql import functions as F

    available = set(df_str.columns)
    projections = []
    missing = []

    for spec in contract.columns:
        if spec.src_name in available:
            projections.append(cast_expr(spec))
        else:
            # Cot khai bao nhung batch nay khong co -> null dung kieu, giu schema on dinh
            missing.append(spec.src_name)
            projections.append(F.lit(None).cast(spec.spark_type()).alias(spec.name))

    if include_raw_payload and available:
        payload_cols = [c for c in contract.src_names if c in available]
        projections.append(
            F.to_json(F.struct(*[F.col(f"`{c}`") for c in payload_cols]))
            .alias(SchemaContract.RAW_PAYLOAD_COL)
        )

    if SchemaContract.CORRUPT_COL in available:
        projections.append(F.col(SchemaContract.CORRUPT_COL))

    if missing:
        logger.warning(json.dumps({
            "event": "contract_columns_missing_in_batch",
            "table": contract.table_name,
            "count": len(missing),
            "columns": missing[:20]
        }))

    unexpected = sorted(
        available - set(contract.src_names) - {"id", SchemaContract.CORRUPT_COL}
    )
    if unexpected:
        logger.warning(json.dumps({
            "event": "columns_not_in_contract",
            "table": contract.table_name,
            "count": len(unexpected),
            "columns": unexpected[:20]
        }))

    return df_str.select(*projections)


def read_conformed(
    spark,
    path: str,
    contract: SchemaContract,
    multi_line: bool = False,
    include_raw_payload: bool = True,
):
    """Doc JSON staging bang schema tuong minh roi conform theo contract.

    Khong dung spark.read.json() truc tiep: viec do bat Spark quet truoc toan
    bo dataset de suy kieu, va ket qua thay doi theo du lieu tung batch
    (cot toan null -> void, so nguyen -> bigint, batch sau co so thap phan ->
    double). Do la nguon goc cua schema drift giua cac partition Parquet.
    """
    reader = (
        spark.read
        .schema(contract.read_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", SchemaContract.CORRUPT_COL)
    )
    if multi_line:
        reader = reader.option("multiLine", "true")

    df_str = reader.json(path)

    logger.info(json.dumps({
        "event": "contract_read_start",
        "table": contract.table_name,
        "path": path,
        "contract_columns": len(contract)
    }))

    return conform(df_str, contract, include_raw_payload=include_raw_payload)


def assert_matches_contract(df, contract: SchemaContract) -> None:
    """Chan som neu DataFrame lech contract - khong de lech lot xuong Parquet."""
    expected = {f.name: f.dataType for f in contract.spark_schema().fields}
    actual = {f.name: f.dataType for f in df.schema.fields}

    problems = []
    for name, dtype in expected.items():
        if name not in actual:
            problems.append(f"thieu cot '{name}'")
        elif actual[name] != dtype:
            problems.append(f"'{name}': mong doi {dtype.simpleString()}, thuc te {actual[name].simpleString()}")

    if problems:
        raise ValueError(
            f"DataFrame khong khop contract '{contract.table_name}': "
            + "; ".join(problems[:10])
            + (f" (+{len(problems) - 10} loi khac)" if len(problems) > 10 else "")
        )


def cast_quality(df_str, contract: SchemaContract, sample_fraction: float = 1.0) -> Dict[str, Any]:
    """Dem so gia tri co du lieu nhung cast ra null - tin hieu API doi shape.

    Khong goi trong duong chay chinh: 364 bieu thuc aggregate mot luot la dat.
    Dung khi debug hoac chay dinh ky de giam sat.
    """
    from pyspark.sql import functions as F

    source = df_str.sample(sample_fraction) if 0 < sample_fraction < 1 else df_str
    available = set(source.columns)
    specs = [s for s in contract.columns if s.src_name in available]

    aggs = []
    for spec in specs:
        raw = unwrap_expr(spec.src_name)
        casted = cast_expr(spec)
        aggs.append(F.sum(F.when(raw.isNotNull(), 1).otherwise(0)).alias(f"{spec.name}__in"))
        aggs.append(
            F.sum(F.when(raw.isNotNull() & casted.isNull(), 1).otherwise(0))
            .alias(f"{spec.name}__lost")
        )

    if not aggs:
        return {"table": contract.table_name, "lossy_columns": []}

    row = source.agg(*aggs).collect()[0].asDict()

    lossy = []
    for spec in specs:
        total = row.get(f"{spec.name}__in") or 0
        lost = row.get(f"{spec.name}__lost") or 0
        if lost:
            lossy.append({
                "column": spec.name,
                "logical": spec.logical,
                "non_null_input": total,
                "cast_to_null": lost,
                "loss_pct": round(100.0 * lost / total, 2) if total else 0.0,
            })

    lossy.sort(key=lambda item: item["cast_to_null"], reverse=True)
    report = {"table": contract.table_name, "lossy_columns": lossy}

    if lossy:
        logger.warning(json.dumps({"event": "contract_cast_loss", **report}))
    else:
        logger.info(json.dumps({
            "event": "contract_cast_clean",
            "table": contract.table_name,
            "checked_columns": len(specs)
        }))
    return report
