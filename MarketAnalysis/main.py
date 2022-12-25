from init import verify_db
import warnings
import time
import os

warnings.filterwarnings("ignore")


def main():
    verify_db()
    while True:
        time.sleep(2*60)
        os.system('python update.py')


if __name__ == '__main__':
    main()
