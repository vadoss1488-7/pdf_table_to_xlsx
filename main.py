from pathlib import Path

import pdfplumber
from openpyxl import Workbook
from tqdm import tqdm


INPUT_DIR = Path(r"pdf_input")
OUTPUT_DIR = Path(r"xlsx_output")


def make_unique_columns(cols):
    result = []
    counter = {}
    for i, c in enumerate(cols):
        if c is None:
            c = ""
        c = str(c).strip()
        if c == "":
            c = f"col_{i}"
        if c in counter:
            counter[c] += 1
            new_name = f"{c}_{counter[c]}"
        else:
            counter[c] = 0
            new_name = c
        result.append(new_name)
    return result


def normalize_table(table):
    if not table or len(table) < 2:
        return None, None

    raw_header = table[0]
    raw_rows = table[1:]

    cleaned_rows = []
    for row in raw_rows:
        if not row:
            continue
        if all((str(c).strip() == "" if c is not None else True) for c in row):
            continue
        cleaned_rows.append(row)

    if not cleaned_rows:
        return None, None

    n_cols = len(raw_header)
    header = make_unique_columns(raw_header)

    rows = []
    for row in cleaned_rows:
        r = list(row)
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        rows.append(r)

    return header, rows


def pdf_to_xlsx_stream(pdf_path: Path, output_dir: Path) -> Path:
    print(f"\n[ФАЙЛ] Начинаю обработку: {pdf_path}")

    pdf_path = pdf_path.expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF-файл не найден: {pdf_path}")

    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{pdf_path.stem}.xlsx"

    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title="data")  

    header_written = False
    total_tables_found = 0
    total_tables_used = 0
    total_rows = 0

    with pdfplumber.open(str(pdf_path)) as pdf:
        print(f"[ФАЙЛ] Страниц в {pdf_path.name}: {len(pdf.pages)}")

        for page_idx, page in enumerate(
            tqdm(pdf.pages, desc=f"  Страницы {pdf_path.name}", unit="стр", leave=False),
            start=1,
        ):
            tables = page.extract_tables()
            if not tables:
                continue

            total_tables_found += len(tables)

            for t_idx, table in enumerate(tables, start=1):
                header, rows = normalize_table(table)
                if header is None or rows is None:
                    continue

                if not header_written:
                    ws.append(header)
                    header_written = True

                for row in rows:
                    ws.append(row)
                    total_rows += 1

                total_tables_used += 1

    print(
        f"[ФАЙЛ] Найдено таблиц: всего={total_tables_found}, "
        f"использовано={total_tables_used}, суммарно строк={total_rows}"
    )

    if not header_written:
        raise ValueError(
            f"Не удалось извлечь ни одной таблицы из файла {pdf_path.name}"
        )

    print(f"[ФАЙЛ] Сохраняю Excel: {output_path} ...")
    wb.save(str(output_path))
    print(f"[ФАЙЛ] Готово, сохранено в: {output_path}")

    return output_path


def process_folder(input_dir: Path, output_dir: Path) -> None:
    print("=== Старт process_folder ===")
    print(f"Текущая рабочая директория: {Path.cwd()}")
    print(f"Входная папка: {input_dir}")
    print(f"Выходная папка: {output_dir}")

    input_dir = input_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()

    print(f"Входная папка (resolve): {input_dir}")
    print(f"Выходная папка (resolve): {output_dir}")

    if not input_dir.exists():
        print(f"[ОШИБКА] Входная папка не найдена: {input_dir}")
        return

    pdf_files = sorted(input_dir.glob("*.pdf"))
    print(f"Найдено {len(pdf_files)} PDF-файлов:")
    for f in pdf_files:
        print(f"  - {f}")

    if not pdf_files:
        print(f"[INFO] В папке {input_dir} не найдено ни одного PDF-файла.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    for pdf in tqdm(pdf_files, desc="PDF-файлы", unit="файл"):
        try:
            out_path = pdf_to_xlsx_stream(pdf, output_dir)
            tqdm.write(f"Готово: {out_path}")
        except Exception as e:
            tqdm.write(f"Ошибка при обработке {pdf.name}: {e}")


def main():
    print("=== Старт скрипта main() ===")
    process_folder(INPUT_DIR, OUTPUT_DIR)
    print("=== Завершение main() ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(repr(e))  