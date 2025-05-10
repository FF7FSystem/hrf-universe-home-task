import argparse
from db import get_session
from statistic_handler import StatisticHandler

THRESHOLD_DEFAULT = 5

def main():
    parser = argparse.ArgumentParser(description="Calculate 'days to hire' statistics and store them in the database.")
    parser.add_argument('--threshold', type=int, default=THRESHOLD_DEFAULT, help='Minimum days_to_hire value to include')
    args = parser.parse_args()

    threshold = args.threshold
    session = get_session()
    StatisticHandler(session).update_posting_statistic(threshold=threshold)


if __name__ == "__main__":
    main()