import argparse
from TransactionManager.transaction_manager import TransactionManager

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputdir', type=str, default='./input/', help='input file name')
    parser.add_argument('--input', type=str, default='input1', help='input file name')

    args = parser.parse_args()

    TM = TransactionManager()
    TM.parser(args.inputdir + args.input)
    TM.print_final_status()
