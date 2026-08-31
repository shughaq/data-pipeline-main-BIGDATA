import argparse
import csv
import random
import sys
from pathlib import Path


def create_sample(input_path, output_path, target_rows, seed=42):
    input_path = Path(input_path)
    if not input_path.exists():
        print(f"[Sample] ERROR: input file not found: {input_path}")
        sys.exit(1)
    if target_rows <= 0:
        raise ValueError("target_rows must be positive")

    rng = random.Random(seed)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open(encoding="utf-8-sig", newline="") as fin:
        reader = csv.reader(fin)
        header = next(reader)
        reservoir = []
        total_rows = 0

        for row in reader:
            total_rows += 1
            if len(reservoir) < target_rows:
                reservoir.append(row)
            else:
                index = rng.randrange(total_rows)
                if index < target_rows:
                    reservoir[index] = row

    with output_path.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(reservoir)

    print(f"[Sample] source: {input_path} ({total_rows} data rows)")
    print(f"[Sample] output: {output_path} ({len(reservoir)} data rows)")
    return output_path, len(reservoir)

def main():
    parser = argparse.ArgumentParser(description="إنشاء عينة صغيرة قابلة لإعادة الإنتاج من ملف CSV ضخم")
    parser.add_argument("--input", required=True, help="مسار الملف الضخم الأصلي")
    parser.add_argument("--output", default="data/orders_sample.csv", help="مسار ملف العينة الناتج")
    parser.add_argument("--rows", type=int, default=100_000, help="عدد الصفوف التقريبي المطلوب في العينة")
    parser.add_argument("--seed", type=int, default=42, help="Seed لثبات النتيجة عند إعادة التشغيل")
    args = parser.parse_args()

    create_sample(args.input, args.output, args.rows, args.seed)


if __name__ == "__main__":
    main()
