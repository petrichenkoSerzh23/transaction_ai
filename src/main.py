from ai_client import GeminiClient 
import os # 
from pathlib import Path
from config import TRANSACTION_FILE
from processor import save_result
from config import TRANSACTION_FILE
from data_loader import (
    check_file_exists,
    transactions_by_mid_and_country,
    get_conversion_by_mid_and_country,
    get_transaction_count_by_mid_with_total,
    get_transactions_by_bank_mid_country,
    get_transactions_by_cc_bin,
    get_top_20_cc_bins,
    get_errors_by_top_25_cc_bins,
    get_top_25_customers_by_errors
)

OUTPUT_DIR = Path("data/output")


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("🚀 Transaction AI starting...\n")

    check_file_exists(TRANSACTION_FILE)
    print(f"File connected: {TRANSACTION_FILE.name}")



    
    df_mid_country = transactions_by_mid_and_country(TRANSACTION_FILE)
    save_result(
        df_mid_country,
        OUTPUT_DIR / "transactions_by_mid_and_country.csv",
        "Количество транзакций в разрезе MID и стран карт."
    )

    
    df_conversion = get_conversion_by_mid_and_country(TRANSACTION_FILE)
    save_result(
        df_conversion,
        OUTPUT_DIR / "conversion_by_mid_and_country.csv",
        "Конверсия платежей по MID и странам карт (success / pending / declined)."
    )

    
    df_mid_total = get_transaction_count_by_mid_with_total(TRANSACTION_FILE)
    save_result(
        df_mid_total,
        OUTPUT_DIR / "transactions_by_mid_total.csv",
        "Количество транзакций по каждому MID, включая итоговое значение."
    )

    
    df_bank_mid_country = get_transactions_by_bank_mid_country(TRANSACTION_FILE)
    save_result(
        df_bank_mid_country,
        OUTPUT_DIR / "transactions_by_bank_mid_country.csv",
        "Распределение транзакций по банкам в разрезе MID и стран карт."
    )

    
    df_cc_bin = get_transactions_by_cc_bin(TRANSACTION_FILE)
    save_result(
        df_cc_bin,
        OUTPUT_DIR / "transactions_by_cc_bin.csv",
        "Количество транзакций по каждому CC BIN с привязкой к стране, банку и MID."
    )

   

    df_bin_errors = get_errors_by_top_25_cc_bins(TRANSACTION_FILE)
    save_result(
        df_bin_errors,
        OUTPUT_DIR / "errors_by_top_25_cc_bins.csv",
        "Статистика ошибок по топ-25 CC BIN."
    )

    
    df_top_customers = get_top_25_customers_by_errors(TRANSACTION_FILE)
    save_result(
        df_top_customers,
        OUTPUT_DIR / "top_25_customers_by_errors.csv",
        "Топ-25 клиентов по количеству транзакций с ошибками."
    )

    print("\n✅ Transaction AI analysis completed successfully")
    try:
        client = GeminiClient()
        
        def get_all_reports_text(directory):
            combined_text = ""
            for file in directory.glob("*.csv"):
                with open(file, 'r', encoding='utf-8') as f:
                    combined_text += f"\n--- Report: {file.name} ---\n"
                    combined_text += f.read() + "\n"
            return combined_text

        reports_data = get_all_reports_text(OUTPUT_DIR)
        
        if reports_data:
            ai_summary = client.analyze_data(reports_data)
            
            print("\n" + "="*50)
            print("GEMINI AI SUMMARY REPORT:")
            print("="*50)
            print(ai_summary)
            print("="*50)
            
            with open(OUTPUT_DIR / "ai_final_summary.txt", "w", encoding="utf-8") as f:
                f.write(ai_summary)
        else:
            print(" No report data found for AI analysis.")

    except Exception as e:
        print(f" AI Analysis failed: {e}")


if __name__ == "__main__":
    main()