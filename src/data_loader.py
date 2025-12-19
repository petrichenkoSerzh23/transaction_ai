from typing import List
import duckdb
from pathlib import Path


def check_file_exists(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    return True


def get_row_count(file_path: Path) -> int:
    con = duckdb.connect(database=":memory:")
    query = f"""
        SELECT COUNT(*) 
        FROM read_csv_auto('{file_path.as_posix()}')
        LIMIT 25
    """
    result = con.execute(query).fetchone()
    con.close()
    return result[0]

def transactions_by_mid_and_country(file_path: Path):
    query = """
    SELECT
        "MID",
        "Card Country",
        COUNT(*) AS total_rows
    FROM read_csv_auto(?)
    WHERE "MID" IS NOT NULL
    GROUP BY "MID", "Card Country"
    ORDER BY "MID", total_rows DESC
    LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query, [str(file_path)]).fetchdf()
    con.close()

    return df


def get_conversion_by_mid_and_country(file_path: Path):
    """
    Подсчёт конверсии по MID и странам карт
    """

    query = """
    SELECT
        "MID",
        "Card Country",
        COUNT(*) FILTER (WHERE "Status" = 'pending')  AS pending_count,
        COUNT(*) FILTER (WHERE "Status" = 'success')  AS success_count,
        COUNT(*) FILTER (WHERE "Status" = 'declined') AS declined_count,
        COUNT(*) AS total_rows,
        ROUND(
            (COUNT(*) FILTER (WHERE "Status" = 'success') * 100.0)
            / NULLIF(COUNT(*), 0),
            2
        ) AS conversion_percent
    FROM read_csv_auto(?)
    WHERE "MID" IS NOT NULL
    GROUP BY "MID", "Card Country"
    ORDER BY "MID", total_rows DESC
    LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query, [str(file_path)]).fetchdf()
    con.close()

    return df


def get_transaction_count_by_mid_with_total(file_path: Path):
    """
    Количество транзакций по каждому MID
    + итоговая строка TOTAL
    """

    query = """
    SELECT
        CASE
            WHEN GROUPING("MID") = 1 THEN 'TOTAL'
            WHEN "MID" IS NULL THEN 'NULL'
            ELSE CAST("MID" AS VARCHAR)
        END AS mid_label,
        COUNT(*) AS total_rows
    FROM read_csv_auto(?)
    GROUP BY ROLLUP("MID")
    ORDER BY
        CASE WHEN GROUPING("MID") = 1 THEN 1 ELSE 0 END,
        mid_label
        LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query, [str(file_path)]).fetchdf()
    con.close()

    return df


def get_transactions_by_bank_mid_country(file_path: Path):
    """
    Количество транзакций по каждому банку
    в разрезе MID и страны карты
    """

    query = """
    SELECT
        "MID",
        "Card Country",
        "Bank Issuer",
        COUNT(*) AS total_rows
    FROM read_csv_auto(?)
    WHERE "MID" IS NOT NULL
    GROUP BY
        "MID",
        "Card Country",
        "Bank Issuer"
    ORDER BY
        "MID",
        "Card Country",
        total_rows DESC,
        "Bank Issuer"
        LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query, [str(file_path)]).fetchdf()
    con.close()

    return df




def get_transactions_by_cc_bin(file_path: Path):
    """
    Количество транзакций по каждому CC bin
    с привязкой к стране карты, банку и MID
    """

    query = """
    SELECT DISTINCT
        "CC bin",
        COUNT(*) OVER (PARTITION BY "CC bin") AS total_rows,
        FIRST_VALUE("Card Country") OVER (PARTITION BY "CC bin") AS "Card Country",
        FIRST_VALUE("Bank Issuer") OVER (PARTITION BY "CC bin") AS "Bank Issuer",
        FIRST_VALUE("MID") OVER (PARTITION BY "CC bin") AS "MID"
    FROM read_csv_auto(?)
    WHERE "CC bin" IS NOT NULL
    ORDER BY total_rows DESC
    LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query, [str(file_path)]).fetchdf()
    con.close()

    return df


def get_top_20_cc_bins(file_path: Path) -> List[str]:
    """
    Возвращает TOP-20 CC bin по количеству транзакций
    """

    query = """
    SELECT
        "CC bin"
    FROM read_csv_auto(?)
    WHERE "CC bin" IS NOT NULL
    GROUP BY "CC bin"
    ORDER BY COUNT(*) DESC
    LIMIT 20
    """

    con = duckdb.connect(database=":memory:")
    result = con.execute(query, [str(file_path)]).fetchall()
    con.close()

    
    return [row[0] for row in result]


def get_bin_error_statistics(file_path: Path):
    """
    Ошибки (Error Unsafe Reason) по TOP-20 CC bin
    """

    top_bins: List[str] = get_top_20_cc_bins(file_path)

    if not top_bins:
        raise ValueError("Список CC bin пуст")

    bins_sql = ", ".join(f"'{bin_}'" for bin_ in top_bins)

    query = f"""
    SELECT DISTINCT
        "CC bin",
        "Error Unsafe Reason",
        COUNT(*) OVER (
            PARTITION BY "CC bin", "Error Unsafe Reason"
        ) AS total_occurrences,
        FIRST_VALUE("Card Country") OVER (PARTITION BY "CC bin") AS "Card Country",
        FIRST_VALUE("Bank Issuer") OVER (PARTITION BY "CC bin") AS "Bank Issuer",
        FIRST_VALUE("MID") OVER (PARTITION BY "CC bin") AS "MID"
    FROM read_csv_auto('{file_path.as_posix()}')
    WHERE "CC bin" IN ({bins_sql})
      AND "Error Unsafe Reason" IS NOT NULL
    ORDER BY
        "CC bin",
        total_occurrences DESC
        LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query).fetchdf()
    con.close()

    return df


def get_errors_by_top_25_cc_bins(file_path: Path):
    """
    Ошибки (Error Unsafe Reason) по TOP-25 CC bin
    """

    query = f"""
    WITH top_bins AS (
        SELECT 
            "CC bin",
            COUNT(*) AS total_rows
        FROM read_csv_auto('{file_path.as_posix()}')
        WHERE "CC bin" IS NOT NULL
        GROUP BY "CC bin"
        ORDER BY total_rows DESC
        LIMIT 25
    )
    SELECT DISTINCT
        t."CC bin",
        t."Error Unsafe Reason",
        COUNT(*) OVER (
            PARTITION BY t."CC bin", t."Error Unsafe Reason"
        ) AS total_occurrences,
        FIRST_VALUE(t."Card Country") OVER (PARTITION BY t."CC bin") AS "Card Country",
        FIRST_VALUE(t."Bank Issuer") OVER (PARTITION BY t."CC bin") AS "Bank Issuer",
        FIRST_VALUE(t."MID") OVER (PARTITION BY t."CC bin") AS "MID"
    FROM read_csv_auto('{file_path.as_posix()}') t
    JOIN top_bins b
        ON t."CC bin" = b."CC bin"
    WHERE t."Error Unsafe Reason" IS NOT NULL
    ORDER BY
        t."CC bin",
        total_occurrences DESC
        LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query).fetchdf()
    con.close()

    return df


def get_top_25_customers_by_errors(file_path: Path):
    """
    Топ-25 (Customer Email) по количеству ошибок
    """

    query = f"""
    SELECT 
        "Customer Email",
        COUNT(*) AS error_count
    FROM read_csv_auto('{file_path.as_posix()}')
    WHERE "Error Unsafe Reason" IS NOT NULL
      AND "Customer Email" IS NOT NULL
    GROUP BY "Customer Email"
    ORDER BY error_count DESC
    LIMIT 25
    """

    con = duckdb.connect(database=":memory:")
    df = con.execute(query).fetchdf()
    con.close()

    return df