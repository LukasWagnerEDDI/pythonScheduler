import sys
import argparse

if __name__ == '__main__':
	parameter_dict = sys.argv[1]
	open(f'{parameter_dict["file_name"]}.json', 'w')