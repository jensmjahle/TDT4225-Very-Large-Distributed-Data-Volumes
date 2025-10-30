from tabulate import tabulate

def print_table(data, headers=None, floatfmt=".2f", max_rows=20):
    if not data:
        print("(no data)")
        return

    # Auto-detect headers
    if headers is None:
        headers = list(data[0].keys())

    # Cut long results
    if len(data) > max_rows:
        data = data[:max_rows]

    table = [[d.get(h, "") for h in headers] for d in data]
    print(tabulate(table, headers=headers, floatfmt=floatfmt, tablefmt="grid"))
