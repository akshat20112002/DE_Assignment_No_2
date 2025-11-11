from source.database import create_table, load_csv
from source.analytics import queries
def main():
    create_table()
    load_csv()
    queries()
if __name__ == "__main__":
    main()