import argparse
import csv
import random
import string
from pathlib import Path


NUM_CUSTOMERS = 50_000
NUM_PURCHASES = 5_000_000


def random_text(min_len: int, max_len: int) -> str:
	length = random.randint(min_len, max_len)
	alphabet = string.ascii_letters + string.digits + " "
	return "".join(random.choice(alphabet) for _ in range(length))


def create_customers(customers_path: Path, seed: int | None = None) -> None:
	if seed is not None:
		random.seed(seed)

	with customers_path.open("w", newline="", encoding="utf-8") as file:
		writer = csv.writer(file)
		writer.writerow(["CustID", "Name", "Age", "Address", "Salary"])

		for cust_id in range(1, NUM_CUSTOMERS + 1):
			name = random_text(10, 20)
			age = random.randint(18, 100)
			address = random_text(12, 40)
			salary = round(random.uniform(1000, 10000), 2)

			writer.writerow([cust_id, name, age, address, salary])


def create_purchases(purchases_path: Path, seed: int | None = None) -> None:
	if seed is not None:
		random.seed(seed)

	with purchases_path.open("w", newline="", encoding="utf-8") as file:
		writer = csv.writer(file)
		writer.writerow(
			["TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc"]
		)

		for trans_id in range(1, NUM_PURCHASES + 1):
			cust_id = random.randint(1, NUM_CUSTOMERS)
			trans_total = round(random.uniform(10, 2000), 2)
			trans_num_items = random.randint(1, 15)
			trans_desc = random_text(20, 50)

			writer.writerow([trans_id, cust_id, trans_total, trans_num_items, trans_desc])


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Generate Customers and Purchases datasets."
	)
	parser.add_argument(
		"--customers-file",
		default="Customers.csv",
		help="Output file path for Customers dataset (default: Customers.csv)",
	)
	parser.add_argument(
		"--purchases-file",
		default="Purchases.csv",
		help="Output file path for Purchases dataset (default: Purchases.csv)",
	)
	parser.add_argument(
		"--seed",
		type=int,
		default=None,
		help="Optional random seed for reproducibility.",
	)
	args = parser.parse_args()

	customers_path = Path(args.customers_file)
	purchases_path = Path(args.purchases_file)

	create_customers(customers_path, args.seed)
	create_purchases(purchases_path, args.seed)

	print(f"Created {NUM_CUSTOMERS:,} customers in: {customers_path}")
	print(f"Created {NUM_PURCHASES:,} purchases in: {purchases_path}")


if __name__ == "__main__":
	main()
