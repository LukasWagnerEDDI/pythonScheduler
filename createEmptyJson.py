import sys
import argparse

def main(*args, **kwargs):
   open(f'{kwargs["file_name"]}.json', 'w')

if __name__ == '__main__':
		
	main(**dict(arg.split('=') for arg in sys.argv[1:]))


	