import argparse
from statistic_handler import StatisticHandler

def main():
    parser = argparse.ArgumentParser(description="Calculate 'days to hire' statistics and store them in the database.")
    parser.add_argument('--threshold', type=int, default=5, help='Minimum days_to_hire value to include')
    args = parser.parse_args()

    threshold = args.threshold
    StatisticHandler.update_posting_statistic(threshold=threshold)


if __name__ == "__main__":
    main()